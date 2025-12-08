/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#pragma once

// pyval is a PyObject* classname is a string
#define AS_Matches_Classname(pyval, classname)                                 \
    (strcmp((pyval)->ob_type->tp_name, (classname)) == 0)

#include <aerospike/as_error.h>

// Append to original error message
#undef as_error_update
#define as_error_update(__err, __code, __fmt, ...)                             \
    {                                                                          \
        if (__err->code != AEROSPIKE_OK) {                                     \
            as_error_set_message(__err, __code, __err->message);               \
            __err->code = __code;                                              \
            char str_to_append[AS_ERROR_MESSAGE_MAX_LEN];                      \
            snprintf(str_to_append, AS_ERROR_MESSAGE_MAX_LEN, __fmt,           \
                     ##__VA_ARGS__);                                           \
            as_error_append(__err, str_to_append);                             \
            else                                                               \
            {                                                                  \
                as_error_setallv(__err, __code, __func__, __FILE__, __LINE__,  \
                                 __fmt, ##__VA_ARGS__);                        \
            }                                                                  \
        }                                                                      \
    }
