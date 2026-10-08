#pragma once

#include<stddef.h>
#include<stdint.h>

// C ABI边界 供其它语言(ctypes/cgo/bindgen...)绑定
#ifdef __cplusplus
extern "C" {
#endif

typedef struct xmr_client xmr_client;

// endpoint是"host:port" 失败返回NULL
xmr_client* xmr_client_connect(const char* endpoint);

void xmr_client_close(xmr_client* client);

// 设置要随任务上传的插件.so本地路径(可选)
void xmr_client_set_plugin(xmr_client* client, const char* pluginPath);

/**
 * 提交任务给master
 * @return 0-master受理任务了
 *         n-被master拒了 reason里是原因
 */
int xmr_client_submit(xmr_client* client, const char* job, uint64_t reducers, uint64_t workers,
                      const char* output, const char* const* inputs, size_t inputCount,
                      char* reason, size_t reasonCapacity);

/**
 * 等结果
 * @return 0-成功
 *         n-错误码 output/reason回填
 */
int xmr_client_wait(xmr_client* client, char* output, size_t outputCapacity,
                    char* reason, size_t reasonCapacity);

// 让master关停
void xmr_client_shutdown(xmr_client* client);

#ifdef __cplusplus
}
#endif
