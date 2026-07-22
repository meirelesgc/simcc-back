import sys
import os
import time
import importlib.util
from simcc.core.logging.events import routine_started, routine_finished, routine_error
from simcc.core.logging.context import routine_name_ctx

def main():
    if len(sys.argv) < 2:
        print("Usage: python run_routine.py <routine_path_relative_to_routines>")
        sys.exit(1)
        
    routine_rel_path = sys.argv[1]
    routine_name = os.path.splitext(os.path.basename(routine_rel_path))[0]
    
    # Resolve the absolute path to the routine script
    routines_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(routines_dir, routine_rel_path)
    
    # Set the routine name context
    routine_name_ctx.set(routine_name)
    
    # Log routine start
    routine_started(routine_name)
    start_time = time.perf_counter()
    
    try:
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location("__main__", script_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load routine from {script_path}")
            
        module = importlib.util.module_from_spec(spec)
        
        # Override sys.argv so that parser / args match direct execution
        sys.argv = [script_path] + sys.argv[2:]
        
        # Put the target script's directory at the top of sys.path to allow local relative imports
        script_dir = os.path.dirname(script_path)
        sys.path.insert(0, script_dir)
        
        # Execute the module code
        spec.loader.exec_module(module)
        
        # Calculate duration and log success
        duration_ms = (time.perf_counter() - start_time) * 1000.0
        routine_finished(routine_name, duration_ms)
        
    except Exception as e:
        # Calculate duration and log error
        duration_ms = (time.perf_counter() - start_time) * 1000.0
        routine_error(routine_name, duration_ms, str(e))
        # Bubble up exit code
        sys.exit(1)

if __name__ == '__main__':
    main()
