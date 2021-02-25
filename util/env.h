//
// Created by wangyi.ywq on 2021/2/25.
//

#ifndef TERARKDB_ENV_H
#define TERARKDB_ENV_H

#ifndef WITH_TERARK_ZIP
#define BYTEDANCE_TERARK_DLL_EXPORT
namespace terark {
BYTEDANCE_TERARK_DLL_EXPORT bool getEnvBool(const char* envName,
                                            bool Default = false);
BYTEDANCE_TERARK_DLL_EXPORT long getEnvLong(const char* envName,
                                            long Default = false);
BYTEDANCE_TERARK_DLL_EXPORT double getEnvDouble(const char* envName,
                                                double Default);
}  // namespace terark
#else
#include "terark/fstring.hpp"
#endif
#endif  // TERARKDB_ENV_H
