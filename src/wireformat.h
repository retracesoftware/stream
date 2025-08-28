#pragma once

namespace retracesoftware_stream {
    // first bit encodes if its a sized type
    // have a intern call on writer, Can cache commonly used strings
    // 
    // 1111XXXX - means fixed-size...
    // we have lookup, which is unsigned, stops at 4 byte
    // we have DEL_1BYTE_INDEX
    // we have DEL_2BYTE_INDEX
    // we have DEL_2BYTE_INDEX
    // deletes run backwards from last index, most deletes are most recent which is smaller more fixed index
    // will compress better also,
    // also have lookup_from_front and lookup_from_back,

    enum SizedTypes : uint8_t {
        BYTES,
        LIST,
        DICT,
        TUPLE,

        STR,
        PICKLED,
        UINT,
        DELETE,
        
        HANDLE,
        BIGINT,
        SET,
        FROZENSET
    };

    // 01111000 0x78

    // #define SIZE_MASK 0x78 

    // FIXEDSIZE = 0xF0

    #define SIZE_MASK 0xF0

    #define ONE_BYTE_SIZE   (0x0B << 4)
    #define TWO_BYTE_SIZE   (0x0C << 4)
    #define FOUR_BYTE_SIZE  (0x0D << 4)
    #define EIGHT_BYTE_SIZE (0x0E << 4)
    #define FIXED_SIZE      (0x0F << 4)

    // #define IS_ROOT_TYPE 0x06

    enum FixedSizeTypes : uint8_t {
        NONE,
        // C_NULL,
        TRUE,
        FALSE,
        FLOAT,
        NEG1,
        // GLOBAL,
        NEW_HANDLE,
        INLINE_NEW_HANDLE,
        PLACEHOLDER,
        REF,
        INT64,

        // just a 64bit pointer
        // EXTREF, // holds the type
        // CACHE_LOOKUP,
        // CACHE_ADD,
    
        // METHOD_DESCRIPTOR,
        FixedSizeTypes__LAST__, // 11
    };

    // enum RootTypes : uint8_t {
    //     UNUSED,
    //     NONE_RESULT, // 0 
    //     TRUE_RESULT, // 0
    //     FALSE_RESULT, // 0
    //     RESULT, // 1        
    //     ERROR, // 2
    //     NEW_INSTANCE, // 1
    //     CHECKPOINT, // 1
    //     DUMPSTACK, // 1
    //     PROXYTYPE, // 2
    //     ENTER, // 1
    //     EXIT, // 1
    //     END, // 0
    //     CALL, // 3
    //     SYNC, // 0
    //     BYTES_WRITTEN,
    //     THREAD_SWITCH, // 1
    //     GC_START,
    //     GC_END,
    //     RootTypes__LAST__
    // };

    // inline uint8_t RootTypeSize(RootTypes type) {
    //     switch(type) {
    //         case THREAD_SWITCH: return 1;
    //         case NONE_RESULT: return 0;
    //         case TRUE_RESULT: return 0;
    //         case FALSE_RESULT: return 0;
    //         case RESULT: return 1;
    //         case ERROR: return 2;
    //         case NEW_INSTANCE: return 1;
    //         case CHECKPOINT: return 1;
    //         case PROXYTYPE: return 2;
    //         // case DELETE: return 1;
    //         case ENTER: return 1;
    //         case EXIT: return 1;
    //         case END: return 0;
    //         case CALL: return 3;
    //         case SYNC: return 0;
    //         default: return 0; // or throw
    //     }
    // }

    // const char * RootTypes_Name(enum RootTypes root);
    const char * FixedSizeTypes_Name(enum FixedSizeTypes root);
}
