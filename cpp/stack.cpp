#include "stream.h"
#include <functional>

#include <internal/pycore_frame.h>
// #define Py_BUILD_CORE
// #include <internal/pycore_opcode.h>

#if PY_VERSION_HEX >= 0x030C0000  // Python 3.12 or higher
static PyObject * get_func(_PyInterpreterFrame * frame) {
    // In Python 3.12+, f_funcobj can be non-function objects for internal frames
    // (e.g., dicts for class definition frames). Return nullptr for unhashable types
    // to avoid "unhashable type: 'dict'" errors in PySet_Contains.
    PyObject * func = frame->f_funcobj;
    // Skip dicts (class definition frames) and other unhashable types
    if (func && !PyDict_Check(func)) {
        return func;
    }
    return nullptr;
}
#else
static PyObject * get_func(_PyInterpreterFrame * frame) {
    return (PyObject *)frame->f_func;
}
#endif

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

    static std::vector<Frame> stack(const set<PyObject *> &exclude, _PyInterpreterFrame * frame) {
        // --- Step 1: Calculate the final size required ---
        size_t count = 0;
        _PyInterpreterFrame * current = frame;
        
        // First pass to count the number of non-excluded frames
        while (current != nullptr) {
            PyObject * func = get_func(current);
            // Skip frames with invalid/non-callable f_funcobj (e.g., class definition frames)
            if (func && !exclude.contains(func)) {
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
            PyObject * func = get_func(current);
            // Skip frames with invalid/non-callable f_funcobj (e.g., class definition frames)
            if (func && !exclude.contains(func)) {
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

    static std::vector<Frame> stack(PyObject * exclude, _PyInterpreterFrame * frame) {
        // --- Step 1: Calculate the final size required ---
        size_t count = 0;
        _PyInterpreterFrame * current = frame;
        
        // First pass to count the number of non-excluded frames
        while (current != nullptr) {
            PyObject * func = get_func(current);
            // Skip frames with invalid/non-callable f_funcobj, or check if excluded
            // PySet_Contains returns -1 on error, 0 if not found, 1 if found
            if (func && PySet_Contains(exclude, func) == 0) {
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
            PyObject * func = get_func(current);
            // Skip frames with invalid/non-callable f_funcobj, or check if excluded
            if (func && PySet_Contains(exclude, func) == 0) {
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

    std::vector<Frame> stack(const set<PyObject *> &exclude) {
        return stack(exclude, get_top_frame());
    }

    static int count_frames(std::function<bool (_PyInterpreterFrame *)> pred, _PyInterpreterFrame * frame) {
        if (!frame) return 0;
        else {
            int prev = count_frames(pred, frame->previous);
            return pred(frame) ? prev + 1 : prev;
        }
    }

    static void fill(std::function<bool (_PyInterpreterFrame *)> pred, PyObject * frames, int index, _PyInterpreterFrame * frame) {
        if (frame) {
            if (pred(frame)) {
                CodeLocation l = Frame(frame->f_code, _PyInterpreterFrame_LASTI(frame) * 2).location();
                
                PyObject * lineno = PyLong_FromLong(l.lineno);
                
                if (!lineno) throw nullptr;
                assert(l.filename);

                PyList_SET_ITEM(frames, index, PyTuple_Pack(2, Py_NewRef(l.filename), lineno));

                fill(pred, frames, index - 1, frame->previous);    
            } else {
                fill(pred, frames, index, frame->previous);
            }
        } else {
            assert(index == -1);
        }
    }

    static PyObject * stack(std::function<bool (_PyInterpreterFrame *)> pred) {
        _PyInterpreterFrame * top = get_top_frame();
        int count = count_frames(pred, top);

        PyObject * frames = PyList_New(count);

        if (count > 0 && frames) {
            try {
                fill(pred, frames, count - 1, top);
            } catch (...) {
                Py_DECREF(frames);
                throw;
            }
        }
        return frames;
    }
    
    PyObject * stack(PyObject *exclude) {
        try {
            return stack([exclude] (_PyInterpreterFrame * frame) {
                PyObject * func = get_func(frame);
                // Skip frames with invalid/non-callable f_funcobj (e.g., class definition frames)
                if (!func) return false;
                switch (PySet_Contains(exclude, func)) {
                    case 0: return true;
                    case 1: return false;
                    default: throw nullptr;
                }
            });
        } catch (...) {
            return nullptr;
        }
    }
    
    static std::tuple<size_t, size_t> update_stack(const set<PyObject *> &exclude, std::vector<Frame> &stack, _PyInterpreterFrame * frame) {

        if (frame) {
            auto [common, index] = update_stack(exclude, stack, frame->previous);

            PyObject * func = get_func(frame);
            // Skip frames with invalid/non-callable f_funcobj (e.g., class definition frames)
            if (!func || exclude.contains(func)) {
                return {common, index};
            }
            
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

    size_t update_stack(const set<PyObject *> &exclude, std::vector<Frame> &stack) {
        auto [common, size] = update_stack(exclude, stack, get_top_frame());
        
        while (size < stack.size()) {
            stack.pop_back();
        }
        return common;
    }

    // struct Stack : public PyObject {

    //     std::vector<Frame> stack;
    //     size_t common;

    //     PyObject * update() {

    //         Stack * delta = reinterpret_cast<Stack *>(type->tp_alloc(StackType, 0));

    //         delta->common = update_stack(..., );
            
    //         ...

    //         return delta;
    //     }
    // };


}