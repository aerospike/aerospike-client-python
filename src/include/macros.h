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

// Cannot use multi-line macro because it cannot return a value.
static inline as_status
as_error_set_or_prepend_helper(as_error *err, as_status code, const char *fmt,
                               const char *func, const char *file,
                               uint32_t line, ...)
{
    if (!fmt) {
        err->code = code;
        goto RETURN_EARLY;
    }

    va_list ap;
    va_start(ap, line);

    char err_msg_to_prepend[AS_ERROR_MESSAGE_MAX_SIZE];
    vsnprintf(err_msg_to_prepend, AS_ERROR_MESSAGE_MAX_SIZE, fmt, ap);

    // Prepend our new error message to the existing one.
    char orig_err_msg[AS_ERROR_MESSAGE_MAX_SIZE];
    strncpy(orig_err_msg, err->message, AS_ERROR_MESSAGE_MAX_LEN);
    // Handles edge case where max number of chars is copied (without null terminator)
    orig_err_msg[AS_ERROR_MESSAGE_MAX_LEN] = '\0';

    as_error_setall(err, code, err_msg_to_prepend, func, file, line);

    if (strlen(orig_err_msg)) {
        as_error_append(err, " -> ");
        as_error_append(err, orig_err_msg);
    }

    va_end(ap);

RETURN_EARLY:
    return code;
}

#undef as_error_update

#define as_error_update(__err, __code, __fmt, ...)                             \
    as_error_set_or_prepend_helper(__err, __code, __fmt, __func__, __FILE__,   \
                                   __LINE__, ##__VA_ARGS__);
