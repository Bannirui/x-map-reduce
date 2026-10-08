#include"xmr/client/capi.h"

#include"xmr/client/client.h"

#include<algorithm>
#include<cstring>
#include<string>

struct xmr_client {
    explicit xmr_client(const std::string& endpoint) : impl(endpoint) {
    }

    xmr::client::Client impl;
};

namespace {
    void copyOut(const std::string& text, char* buffer, std::size_t capacity) {
        if (buffer == nullptr || capacity == 0) {
            return;
        }
        const std::size_t count = std::min(capacity - 1, text.size());
        std::memcpy(buffer, text.data(), count);
        buffer[count] = '\0';
    }
} // namespace

extern "C" {
    xmr_client* xmr_client_connect(const char* endpoint) {
        try {
            return new xmr_client(endpoint == nullptr ? "" : endpoint);
        } catch (...) {
            return nullptr;
        }
    }

    void xmr_client_close(xmr_client* client) {
        delete client;
    }

    int xmr_client_submit(xmr_client* client, const char* job, uint64_t reducers, uint64_t workers,
                          const char* output, const char* const* inputs, size_t inputCount,
                          char* reason, size_t reasonCapacity) {
        if (client == nullptr) {
            return 2;
        }
        try {
            xmr::client::SubmitRequest request;
            request.job = job == nullptr ? "" : job;
            request.reducers = reducers;
            request.workers = workers;
            request.output = output == nullptr ? "" : output;
            for (std::size_t i = 0; i < inputCount; ++i) {
                request.inputs.emplace_back(inputs[i] == nullptr ? "" : inputs[i]);
            }
            std::string why;
            const bool accepted = client->impl.submit(request, why);
            copyOut(why, reason, reasonCapacity);
            return accepted ? 0 : 1;
        } catch (const std::exception& error) {
            copyOut(error.what(), reason, reasonCapacity);
            return 2;
        }
    }

    int xmr_client_wait(xmr_client* client, char* output, size_t outputCapacity,
                        char* reason, size_t reasonCapacity) {
        if (client == nullptr) {
            return 2;
        }
        try {
            const auto result = client->impl.wait();
            copyOut(result.output, output, outputCapacity);
            copyOut(result.reason, reason, reasonCapacity);
            return result.status == xmr::protocol::StatusCode::Ok ? 0 : 1;
        } catch (const std::exception& error) {
            copyOut(error.what(), reason, reasonCapacity);
            return 2;
        }
    }

    void xmr_client_shutdown(xmr_client* client) {
        if (client != nullptr) {
            client->impl.shutdown();
        }
    }
} // extern "C"
