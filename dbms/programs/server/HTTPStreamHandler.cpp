#include "HTTPStreamHandler.h"

#include <Poco/DeflatingStream.h>
#include <Common/setThreadName.h>

#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"

DB::HTTPStreamHandler::HTTPStreamHandler(IServer & server_)
    : server(server_)
    , log(&Logger::get("HTTPStreamHandler"))
{

}

void DB::HTTPStreamHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("HTTPStream");
    Context context = server.context();
    Settings & settings = context.getSettingsRef();

    if (!validate(request, response)) 
    {
        return;
    }
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK);
    response.setChunkedTransferEncoding(true);

    // TODO
    response.set("Access-Control-Allow-Origin", "*");

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
    {
        return;
    }

    response.set("Content-Type", "text/event-stream");
    response.set("Transfer-Encoding", "chunked");

    bool compress = false;
    Poco::DeflatingStreamBuf::StreamType stream_type = Poco::DeflatingStreamBuf::STREAM_GZIP;

    if(request.hasToken("Accept-Encoding", "gzip"))
    {
        compress = true;
        stream_type = Poco::DeflatingStreamBuf::STREAM_GZIP;
        response.set("Content-Encoding", "gzip");
    }
    else if (request.hasToken("Accept-Encoding", "deflate"))
    {
        compress = true;
        stream_type = Poco::DeflatingStreamBuf::STREAM_ZLIB;
        response.set("Content-Encoding", "deflate");
    }

    std::ostream & response_stream = response.send();
    std::deque<std::reference_wrapper<std::ostream>> flush_streams = { std::ref(response_stream) };
    HTTPQueryState query_state(flush_streams);
    Poco::DeflatingOutputStream compress_stream(response_stream, stream_type);

    if (compress)
    {
        flush_streams.push_front(compress_stream);
    }

    process(request, query_state);
}

void DB::HTTPStreamHandler::process(Poco::Net::HTTPServerRequest & request, HTTPQueryState & query_state)
{
    query_state.sendEvent("summary", "I plan to send\n101\nrows");
    query_state.sendEvent("", "col1;col2;col3");

    ThreadFromGlobalPool progress_thread([&query_state]
    {
        for (int i = 0; i <= 100; ++i)
        {
            query_state.sendEvent("progress", std::to_string(i).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    ThreadFromGlobalPool data_thread([&query_state]
    {
        for (int i = 0; i <= 100; ++i)
        {
            query_state.sendEvent("", "0,1,0");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    progress_thread.join();
    data_thread.join();

    query_state.sendEvent("summary", "Done!");
}

bool DB::HTTPStreamHandler::validate(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    return true;
}
