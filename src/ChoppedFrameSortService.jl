module ChoppedFrameSortService
using TOML
using FITSIO
using Aeron
using LinearAlgebra
using ThreadPinning
using Oxygen
using SwaggerMarkdown
using HTTP
using StructTypes
using LinearAlgebra

include("wire-format.jl")

const DType = Float32

# Hold variables used by the loop so we can update all at once
mutable struct State
    last_unfringed::Matrix{DType}
    last_fringed::Matrix{DType}
    flux_averaging_filt_gain::Float32
    even_frame_flux_ave::Float32
    odd_frame_flux_ave::Float32
end

# Add a supporting struct type definition so JSON3 can serialize & deserialize automatically
StructTypes.StructType(::Type{State}) = StructTypes.Struct()

"""
    main()

The main entrypoint of the Low Order Wavefront Sensor service.

Set up a loop on the interactive thread that subscribes to the necessary
aeron streams.
"""
function main(ARGS=ARGS) # Take command line arguments or allow them to be passed in
    @info "Starting choppeframesortservice"

    if Threads.nthreads(:interactive) < 1
        @warn "Ensure Julia is started with at least one interative thread for performance reasons (--threads=1,1)"
    end

    if length(ARGS) < 1
        error("missing configuration toml filepath required argument")
    end

    configname = ARGS[1]
    config = TOML.parsefile(configname)

    pinthreads(config["pin-threads"])

    BLAS.set_num_threads(config["blas-threads"])

    aeron_input_stream_config = AeronConfig(
        channel=config["input-channel"],
        stream=config["input-stream"],
    )

    aeron_output_stream_config_unfringed = AeronConfig(
        channel=config["output-channel-unfringed"],
        stream=config["output-stream-unfringed"],
    )

    aeron_output_stream_config_fringed = AeronConfig(
        channel=config["output-channel-fringed"],
        stream=config["output-stream-fringed"],
    )

    aeron_output_stream_config_diff = AeronConfig(
        channel=config["output-channel-diff"],
        stream=config["output-stream-diff"],
    )

    rest_api_port = config["rest-api-port"]

    # We will resize these after we receive our first image
    last_unfringed = zeros(DType, (0, 0))
    last_fringed = zeros(DType, (0, 0))

    flux_averaging_filt_gain = config["state"]["flux-averaging-filt-gain"]
    even_frame_flux_ave = zero(Float32)
    odd_frame_flux_ave = zero(Float32)

    initial_state = State(
        last_unfringed,
        last_fringed,
        flux_averaging_filt_gain,
        even_frame_flux_ave,
        odd_frame_flux_ave
    )
    current_state_slot = Ref(initial_state)

    register_rest_api(config, current_state_slot)

    looptask = Threads.@spawn :default loopmanager(
        aeron_input_stream_config,
        aeron_output_stream_config_unfringed,
        aeron_output_stream_config_fringed,
        aeron_output_stream_config_diff,
        current_state_slot
    )

    # Watch and report any errors here
    # In future we can write our own error handling, but this way exceptions don't silenty disappear.
    errormonitor(looptask)

    # start the web server
    resttask = Threads.@spawn :interactive serve(; port=rest_api_port)

    try
        run(`systemd-notify --ready`)
    catch
    end


    # TODO: we don't have a way to cleanly shutdown everything yet
    wait(looptask)
    wait(resttask)

    # Can't get here unless we hit an error
    return 0
end


# This function should be called on an interactive thread
function loopmanager(
    aeron_input_stream_config,
    aeron_output_stream_config_unfringed,
    aeron_output_stream_config_fringed,
    aeron_output_stream_config_diff,
    current_state_slot,
)
    @info "Starting loop" Threads.threadpool() Threads.nthreads() Threads.threadid()

    # Subscribe to the incoming stream
    Aeron.subscribe(aeron_input_stream_config) do subscription

        @info "Subscribed to aeron stream" aeron_input_stream_config

        first_message = VenomsImageMessage(first(subscription).buffer)
        img_in = Image(DType, first_message)
        last_i = img_in[1,1]

        # TODO: don't modify, just create a new State object
        current_state_slot[].last_unfringed = zeros(DType, size(img_in))
        current_state_slot[].last_fringed = zeros(DType, size(img_in))

        # Prepare our publication stream: where we output measurements
        publication_unfringed = Aeron.publisher(aeron_output_stream_config_unfringed)
        publication_fringed = Aeron.publisher(aeron_output_stream_config_fringed)
        publication_diff = Aeron.publisher(aeron_output_stream_config_diff)

        @info "Publishing unfringed images to aeron stream " aeron_output_stream_config_unfringed
        @info "Publishing fringed images to aeron stream " aeron_output_stream_config_fringed
        @info "Publishing diff images to aeron stream " aeron_output_stream_config_diff

        # Set up our data to publish (we can use this in an alternating fashion with all three channels)
        pub_buffer = zeros(UInt8, 512 * 640 * 4 + 53)
        pub_header = VenomsImageMessage(pub_buffer)
        SizeX!(pub_header, SizeX(first_message))
        SizeY!(pub_header, SizeY(first_message))
        Format!(pub_header, 9) # Float32
        MetadataLength!(pub_header, 0)
        ImageBufferLength!(pub_header, ImageBufferLength(first_message))
        pub_data = Image(pub_header)

        # Now that we've done all the set up work, enable GC logging so we can keep an eye on things
        GC.enable_logging()

        # Ensure we have at least N frames received before we start forwarding.
        # We need to see a few before we can classify incoming frames with any 
        # reliability
        ready_to_send = false
        counter = 0
        for framereceived in subscription
            if counter > 10
                ready_to_send = true
            else
                counter += 1
            end
            # Read the current state of loop in one go, so it can't change
            # while we are working through one iteration
            state = current_state_slot[]

            # Decode message
            message = VenomsImageMessage(framereceived.buffer)
            Image!(img_in, message)

            # Use image index from tag pixel
            i = img_in[1, 1]

            if last_i != i - 1
                # TODO: handle wrap around
                @warn "non-consecutive frame numbers (dropping frame)" last_i i
                last_i = i
                continue
            end

            frame_total = sum(Float32, @view img_in[2:end,2:end]) # Avoid summing over tag pixels in corner
            if iseven(i)
                residual = frame_total - state.even_frame_flux_ave
                state.even_frame_flux_ave = state.even_frame_flux_ave + residual*state.flux_averaging_filt_gain
            else
                residual = frame_total - state.odd_frame_flux_ave
                state.odd_frame_flux_ave = state.odd_frame_flux_ave + residual*state.flux_averaging_filt_gain
            end

            TimestampNs!(pub_header, TimestampNs(message))
            pub_data .= img_in

            # Based on rolling average, pick the brighter of the even images
            # or odd images as the fringed image. Unfringed is the other.
            if state.even_frame_flux_ave > state.odd_frame_flux_ave
                if iseven(i)
                    # If the even frames are brighter and this is an even frame, publish it as a fringed frame
                    if ready_to_send
                        status = Aeron.publication_offer(publication_fringed, pub_header.buffer)
                    end
                    state.last_fringed .= img_in
                else
                    # If the even frames are brighter and this is an odd frame, publish it as an unfringed frame
                    if ready_to_send
                        status = Aeron.publication_offer(publication_unfringed, pub_header.buffer)
                    end
                    state.last_unfringed .= img_in
                end
            else
                if iseven(i)
                    # If the odd frames are brighter and this is an even frame, publish it as a fringed frame
                    if ready_to_send
                        status = Aeron.publication_offer(publication_unfringed, pub_header.buffer)
                    end
                    state.last_unfringed .= img_in
                else
                    # If the odd frames are brighter and this is an odd frame, publish it as an unfringed frame
                    if ready_to_send
                        status = Aeron.publication_offer(publication_fringed, pub_header.buffer)
                    end
                    state.last_fringed .= img_in
                end
            end

            # Now publish a rolling differential image using the most recent fringed and unfringed pairs.
            pub_data .= state.last_fringed .- state.last_unfringed
            if ready_to_send
                status = Aeron.publication_offer(publication_diff, pub_header.buffer)
            end

            last_i = i
        end
        close(publication_unfringed)
        close(publication_fringed)
        close(publication_diff)
    end
end


function cog(img)
    tot = zero(eltype(img))
    X = zero(eltype(img))
    Y = zero(eltype(img))
    for xi in axes(img, 1), yi in axes(img, 2)
        X += xi * img[xi, yi]
        Y += yi * img[xi, yi]
        tot += img[xi, yi]
    end
    return X / tot, Y / tot
end

## REST API ##
function register_rest_api(config, current_state_slot)

    # Note for the technically inclined: Oxygen.jl is wrapping HTTP.jl.
    # These macros are adding request handlers to a default global route
    # stored in Oxygen.ROUTER. That's how the server knows about them.
    # This approach would not allow multiple servers on different ports

    @get "/" function (req::HTTP.Request)
        # Home page message
        return html("<h1>choppeframesortservice</h1> <a href=\"/docs\">/docs</a>")
    end


    @get "/state" function (req::HTTP.Request)
        return current_state_slot[]
    end

    # Atomically load new ref image and matrices, swap at next loop iteration.
    @post "/state" function (req::HTTP.Request)
        qp = queryparams(req)
        # current_state = current_state_slot[] # Not currently needed

        path = get(qp, "reference-image", config["state"]["reference-image"])
        reference_image = FITS(path, "r") do fitsfile
            read(fitsfile[1])
        end
        path = get(qp, "dark", config["state"]["dark"])
        dark = FITS(path, "r") do fitsfile
            read(fitsfile[1])
        end
        path = get(qp, "image-mask", config["state"]["image-mask"])
        image_mask = FITS(path, "r") do fitsfile
            Bool.(read(fitsfile[1]))
        end
        path = get(qp, "cog-image-mask", config["state"]["image-mask"])
        cog_image_mask = FITS(path, "r") do fitsfile
            Bool.(read(fitsfile[1]))
        end
        image_scratch_valid = zeros(DType, count(image_mask))
        image_scratch = zeros(DType, size(image_mask))
        path = get(qp, "image-to-modes", config["state"]["image-to-modes"])
        image_to_modes = FITS(path, "r") do fitsfile
            read(fitsfile[1])
        end
        path = get(qp, "slopes-to-TT", config["state"]["slopes-to-TT"])
        slopes_to_TT = FITS(path, "r") do fitsfile
            read(fitsfile[1])
        end
        path = get(qp, "modes-to-actus", config["state"]["modes-to-actus"])
        modes_to_actus = FITS(path, "r") do fitsfile
            read(fitsfile[1])
        end
        path = get(qp, "cog-reference", config["state"]["cog-reference"])
        cog_reference = FITS(path, "r") do fitsfile
            read(fitsfile[1])
        end
        modal_coefficients = zeros(DType, size(image_to_modes, 2))
        actus_coefficients = zeros(DType, size(modes_to_actus, 2))

        new_state = State(reference_image[image_mask], dark, image_mask, cog_image_mask, image_to_modes, modes_to_actus, slopes_to_TT, cog_reference, image_scratch_valid, image_scratch, modal_coefficients, actus_coefficients)
        current_state_slot[] = new_state

        return true
    end

    @get "/shutdown" function (req::HTTP.Request)
        # TODO: signal loop that it needs to exit cleanly. For now we'll just quit.
        exit(0)
    end

end

# have processing "state"
# other states:
# initialization
# error state or well outside of rate -> jump to idle state

end