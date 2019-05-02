from .utils import log, capture_7zip_stdout, query_yes_no
import os
if os.name == 'nt':
    from .utils import find_program_win as find_program
else:
    from .utils import find_program_other as find_program
