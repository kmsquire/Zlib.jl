
import Zlib: z_stream, init_decompress, libz,
             Z_SYNC_FLUSH, Z_OK, Z_STREAM_END, Z_DATA_ERROR, Z_BUF_ERROR

type ZStream
    strm::z_stream
    input::Vector{Uint8}
    output::Vector{Uint8}
    chunksize::Int
    append::Bool

    function ZStream(input::Vector{Uint8}, output::Vector{Uint8}, raw::Bool, append::Bool, chunksize::Int)
        strm = init_decompress()

        strm.avail_in = length(input)
        strm.next_in = input

        strm.avail_out = length(output)
        strm.next_out = output

        x = new(strm, input, output, chunksize, append)
    end
end

ZStream(input::Vector{Uint8}, output::Vector{Uint8}; 
        raw::Bool=false, append::Bool=false, chunksize::Int=Zlib.CHUNKSIZE) = 
    ZStream(input, output, raw, append, chunksize)

function set_input(stream::ZStream, input::Vector{Uint8})
    if stream.strm.avail_in > 0
        error("Error: Cannot set z_stream input: previous input buffer still has $(stream.strm.avail_in) bytes available")
    end

    stream.input = input   # keep a handle on the input
    stream.strm.avail_in = length(input)
    stream.strm.next_in = input

    stream
end

function set_output(stream::ZStream, output::Vector{Uint8}, pos::Int=1)
    stream.output = output  # keep a handle on the output
    stream.strm.next_out = pointer(output,pos)
    stream.strm.avail_out = length(output)-pos+1

    stream
end


function notify_filled(buffer::IOBuffer, n::Int)
    if buffer.append
        buffer.size += n
    else
        buffer.ptr += n
    end
end


function decompress_next(stream::ZStream)
    # allocate more output space if possible/necessary
    if stream.append && stream.strm.avail_out == 0
        len = length(stream.output)
        resize!(stream.output, len+stream.chunksize)
        stream.strm.avail_out = stream.chunksize
        stream.strm.next_out = pointer(stream.output, len+1)
    end

    start_sz = stream.strm.total_out
    
    ret = ccall((:inflate, libz),
                Int32, (Ptr{z_stream}, Int32),
                &(stream.strm), Z_SYNC_FLUSH)

    if ret == Z_DATA_ERROR
        error("Error: input is not zlib compressed data: $(bytestring(stream.strm.msg))")
    elseif ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR
        error("Error in zlib inflate stream ($(ret)).")
    end

    # fix up output buffer
    nout = stream.strm.total_out - start_sz
    if nout > 0
        notify_filled(x.buffer, nout)
    end    

    ret
end

nb_available(stream::ZStream) = nb_available(stream.buffer)
nb_available_in(stream::ZStream) = stream.strm.avail_in

function run_decompress()
    @schedule begin
        while true
            status = wait(io_state_change)
            if status == :input_data_available || :output_buffer_available
                while nb_available(x.io) && !full(output_buffer)
                    n = min(nb_available(x.io), x.chunksize, 1)
                    resize!(x.rawbuf, n)
                    read(x.io, x.rawbuf)
                    
                    decompress_next(x.strm)
                end
            elseif status == :input_eof
                break
            end
            notify(decompress_ready)
        end
    end

    @schedule begin
        while true
            if eof(x.io)
                notify(io_state_change, :input_eof)
                break
            end

            start_reading(x.io)
            wait_readnb(x.io, 1)
            notify(io_state_change, :input_data_available)
            yield()
            wait(decompress_ready)
        end
    end

end
