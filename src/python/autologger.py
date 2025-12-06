import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime

import constants


@contextmanager
def capture_output(log_dir=constants.TOOL_LOGS_DIR):
  """Captures all stdout/stderr to a centralized log file."""
  os.makedirs(log_dir, exist_ok=True)

  # Generate log filename with timestamp
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  tool_name = os.path.basename(sys.argv[0])
  log_file_path = os.path.join(log_dir, f"{tool_name}_{timestamp}.log")

  start_time = time.time()
  start_datetime = datetime.now()

  # Open log file once and keep it open
  log_file = open(log_file_path, "w", buffering=1)  # line buffering
  print(f"\nAUTOLOGGER: logging all output to {log_file_path}\n")

  # Write header
  log_file.write(f"Tool: {tool_name}\n")
  log_file.write(f"Start Time: {start_datetime.isoformat()}\n")
  log_file.write(f"Working Directory: {os.getcwd()}\n")
  log_file.write(f"Command: {' '.join(sys.argv)}\n")
  log_file.write("-" * 80 + "\n\n")
  log_file.flush()

  # Tee class to write to both original stream and log
  class Tee:
    def __init__(self, stream, log_file):
      self.stream = stream
      self.log_file = log_file

    def write(self, data):
      self.stream.write(data)
      self.log_file.write(data)

    def flush(self):
      self.stream.flush()
      self.log_file.flush()

  # Save original streams
  old_stdout = sys.stdout
  old_stderr = sys.stderr

  exit_code = 0
  exception_info = None

  try:
    # Both Tee instances share the same log_file handle
    sys.stdout = Tee(old_stdout, log_file)
    sys.stderr = Tee(old_stderr, log_file)
    yield log_file_path
  except SystemExit as e:
    exit_code = e.code if e.code is not None else 0
    raise
  except Exception as e:
    exit_code = 1
    exception_info = traceback.format_exc()
    raise
  finally:
    print(f"\nAUTOLOGGER: see all output in {log_file_path}\n")

    sys.stdout = old_stdout
    sys.stderr = old_stderr

    # Write footer with timing and exit info
    elapsed = time.time() - start_time
    end_datetime = datetime.now()

    log_file.write("\n" + "-" * 80 + "\n")
    log_file.write(f"End Time: {end_datetime.isoformat()}\n")
    log_file.write(f"Duration: {elapsed:.3f} seconds\n")
    log_file.write(f"Exit Code: {exit_code}\n")
    if exception_info:
      log_file.write(f"\nException Traceback:\n{exception_info}")

    log_file.close()
