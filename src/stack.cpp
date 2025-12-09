#include "stream.h"

#include <internal/pycore_frame.h>
// #define Py_BUILD_CORE
// #include <internal/pycore_opcode.h>

namespace retracesoftware_stream {

    // int _PyInterpreterFrame_GetLine(_PyInterpreterFrame *frame)
    // {
    //     int addr = _PyInterpreterFrame_LASTI(frame) * 2;
    //     return PyCode_Addr2Line(frame->f_code, addr);
    // }

    int stacksize(_PyInterpreterFrame * frame) {
        return frame ? stacksize(frame->previous) + 1 : 0;
    }

    static _PyInterpreterFrame * get_top_frame() {
        return PyThreadState_Get()->cframe->current_frame;
    }

    // static PyCodeObject * find_in_stack(_PyInterpreterFrame * frame, std::function<bool (PyCodeObject *)> pred) {
    //     if (!frame) return nullptr;
    //     else if (pred(frame->f_code)) return frame->f_code;
    //     else return find_in_stack(frame->previous, pred);
    // }

    // PyCodeObject * find_in_stack(std::function<bool (PyCodeObject *)> pred) {
    //     return find_in_stack(get_top_frame(), pred);
    // }

    // PyObject * create_python_frame(Frame frame) {
    //     PyObject * result = PyDict_New();

    //     assert(result);
    //     assert(frame.code_object);
    //     PyDict_SetItemString(result, "code", Py_NewRef((PyObject *)frame.code_object));
    //     PyDict_SetItemString(result, "instruction", PyLong_FromLong((long)frame.instruction));

    //     return result;
    // }

    // PyObject * create_python_stack(std::vector<Frame> &stack) {

    //     PyObject * result = PyList_New(stack.size());

    //     for (size_t i = 0; i < stack.size(); i++) {
    //         PyList_SetItem(result, i, create_python_frame(stack[i]));
    //     }
    //     return result;
    // }

    // int instr_size(_Py_CODEUNIT * current) {
    //     int size = 0;
    //     while (_Py_OPARG(current[size]) == EXTENDED_ARG || _Py_OPARG(current[size]) == EXTENDED_ARG_QUICK) size++;
    //     return size + 1;
    // }

    static std::vector<Frame> stack(const set<PyFunctionObject *> &exclude, _PyInterpreterFrame * frame) {
        // --- Step 1: Calculate the final size required ---
        size_t count = 0;
        _PyInterpreterFrame * current = frame;
        
        // First pass to count the number of non-excluded frames
        while (current != nullptr) {
            if (!exclude.contains(current->f_func)) {
                count++;
            }
            current = current->previous;
        }

        // --- Step 2: Allocate and populate the vector ---
        std::vector<Frame> result_vec;
        result_vec.reserve(count);
        current = frame; // Reset to the starting frame

        // Second pass to populate the vector
        // This populates the vector in reverse order (deepest frames first), 
        // matching the behavior of the original recursive solution's push_back.
        while (current != nullptr) {
            if (!exclude.contains(current->f_func)) {
                // Note: Since we are iterating backward (from current frame back to main),
                // and push_back adds to the end, the resulting vector will be ordered
                // from the deepest frame to the outermost frame (matching the original).
                result_vec.push_back(Frame(current->f_code, _PyInterpreterFrame_LASTI(current) * 2));
            }
            current = current->previous;
        }

        // Since the original recursive function was called with the outermost frame first,
        // we need to reverse the result_vec to match the call order of the original (deepest last).
        // The original code pushed frames in the order they were processed *during the return phase*.
        // The recursive return phase naturally reverses the order.

        // Let's assume the desired final order is from the *oldest* frame to the *newest* frame.
        // The original code's push_back created a vector from the oldest frame to the newest.
        // If the original order is desired: reverse the vector after population.
        std::reverse(result_vec.begin(), result_vec.end()); 
        // If you want the deepest frame first, you would omit std::reverse.
        
        return result_vec;
    }

    std::vector<Frame> stack(const set<PyFunctionObject *> &exclude) {
        return stack(exclude, get_top_frame());
    }

    static std::tuple<size_t, size_t> update_stack(set<PyFunctionObject *> &exclude, std::vector<Frame> &stack, _PyInterpreterFrame * frame) {

        if (frame) {
            auto [common, index] = update_stack(exclude, stack, frame->previous);

            if (exclude.contains(frame->f_func)) {
                return {common, index};
            }

            assert(frame->f_func);
            
            // if (instr_size(frame->prev_instr) > 1) {
            //     raise(SIGTRAP);
            // }

            Frame f(frame->f_code, _PyInterpreterFrame_LASTI(frame) * 2);
            // Frame f(frame->f_code, _PyInterpreterFrame_LASTI(frame) + instr_size(frame->prev_instr));

            // printf("update_stack, common: %i index: %i, qualname: %s, inst: %i\n", common, index, PyUnicode_AsUTF8(frame->f_code->co_qualname), f.instruction);

            if (index == stack.size()) {
                stack.push_back(f);
                return {common, index + 1};
            }
            else if (common == index && stack[index] == f) {
                return {common + 1, index + 1};
            } else {
                stack[index] = f;
                return {common, index + 1};
            }
        } else {
            return {0, 0};
        }
    }

    size_t update_stack(set<PyFunctionObject *> &exclude, std::vector<Frame> &stack) {
        auto [common, size] = update_stack(exclude, stack, get_top_frame());
        
        while (size < stack.size()) {
            stack.pop_back();
        }
        return common;
    }
}