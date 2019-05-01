from .utils import log, capture_7zip_stdout
import os
if os.name == 'nt':
    from .utils import find_program_win as find_program
else:
    from .utils import find_program_other as find_program
