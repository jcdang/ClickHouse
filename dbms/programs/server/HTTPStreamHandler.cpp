#include <Poco/DeflatingStream.h>
#include "IServer.h"
#include "HTTPStreamHandler.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
namespace DB {

HTTPStreamHandler::HTTPStreamHandler(IServer & server_) : server(server_)
{

}

void HTTPStreamHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) 
{
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

    bool compress_gzip = false;
    bool compress_deflate = false;

    if(request.hasToken("Accept-Encoding", "gzip"))
    {
        compress_gzip = true;
        response.set("Content-Encoding", "gzip");
    }
    else
    if (request.hasToken("Accept-Encoding", "deflate"))
    {
        compress_deflate = true;
        response.set("Content-Encoding", "deflate");
    }

    std::ostream & response_stream = response.send();

    if (compress_gzip)
    {
        Poco::DeflatingOutputStream gzip_stream(response_stream, Poco::DeflatingStreamBuf::STREAM_GZIP);
        process(request, gzip_stream, [&gzip_stream, &response_stream]()
        {
            gzip_stream.flush();
            response_stream.flush();
        });
        gzip_stream.close();
    }
    else if (compress_deflate)
    {
        Poco::DeflatingOutputStream zlib_stream(response_stream, Poco::DeflatingStreamBuf::STREAM_ZLIB);
        process(request, zlib_stream, [&zlib_stream, &response_stream]()
        {
            zlib_stream.flush();
            response_stream.flush();
        });
        zlib_stream.close();
    }
    else
    {
        process(request, response_stream, [&response_stream]()
        {
            response_stream.flush();
        });
    }
}

void HTTPStreamHandler::process(Poco::Net::HTTPServerRequest & request, std::ostream & out, const std::function<void()> flush)
{
    out << "event: summary\n";
    out << "data: I plan to send\n";
    out << "data: 101\n";
    out << "data: rows\n\n";
    flush();

    out << "event: progress\n";
    out << "data: 0\n\n";
    flush();

    out << "data: col1;col2;col3\n\n";
    flush();

    for (int i = 0; i <= 100; ++i)
    {
        out << "event: progress\n";
        out << "data: " << i << "\n\n";
        flush();

        out << "data: 0,0,0\n\n";
        flush();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    out << "event: summary\n";
    out << "data: Done\n\n";
    flush();
}

bool HTTPStreamHandler::validate(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    return true;
}

}

#pragma GCC diagnostic pop