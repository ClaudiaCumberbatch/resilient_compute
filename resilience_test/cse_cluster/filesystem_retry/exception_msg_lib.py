'''
Reference: https://docs.python.org/3.10/library/exceptions.html#bltin-exceptions
The annotation 'sp' indicates sporadic error, 're' indicates resource error.
(Annotated by me, likely to be wrong).

Python 3.10 built-in exceptions class hierarchy:
BaseException
 +-- SystemExit
 +-- KeyboardInterrupt
 +-- GeneratorExit (sp)
 +-- Exception
      +-- StopIteration (sp)
      +-- StopAsyncIteration (sp)
      +-- ArithmeticError
      |    +-- FloatingPointError
      |    +-- OverflowError
      |    +-- ZeroDivisionError
      +-- AssertionError (sp)
      +-- AttributeError
      +-- BufferError
      +-- EOFError
      +-- ImportError
      |    +-- ModuleNotFoundError
      +-- LookupError
      |    +-- IndexError
      |    +-- KeyError
      +-- MemoryError (sp)
      +-- NameError
      |    +-- UnboundLocalError
      +-- OSError (also likely to be resource error)
      |    +-- BlockingIOError
      |    +-- ChildProcessError (sp)
      |    +-- ConnectionError (re)
      |    |    +-- BrokenPipeError
      |    |    +-- ConnectionAbortedError
      |    |    +-- ConnectionRefusedError
      |    |    +-- ConnectionResetError
      |    +-- FileExistsError
      |    +-- FileNotFoundError
      |    +-- InterruptedError (sp)
      |    +-- IsADirectoryError
      |    +-- NotADirectoryError
      |    +-- PermissionError (sp)
      |    +-- ProcessLookupError (sp)
      |    +-- TimeoutError (sp)
      +-- ReferenceError
      +-- RuntimeError
      |    +-- NotImplementedError
      |    +-- RecursionError (sp)
      +-- SyntaxError
      |    +-- IndentationError
      |         +-- TabError
      +-- SystemError
      +-- TypeError
      +-- ValueError
      |    +-- UnicodeError
      |         +-- UnicodeDecodeError
      |         +-- UnicodeEncodeError
      |         +-- UnicodeTranslateError
      +-- Warning (ignore)
           +-- DeprecationWarning
           +-- PendingDeprecationWarning
           +-- RuntimeWarning
           +-- SyntaxWarning
           +-- UserWarning
           +-- FutureWarning
           +-- ImportWarning
           +-- UnicodeWarning
           +-- BytesWarning
           +-- EncodingWarning
           +-- ResourceWarning
'''

ERROR_LIST = [
    'SystemExit',
    'KeyboardInterrupt',
    'FloatingPointError',
    'OverflowError',
    'ZeroDivisionError',
    'AttributeError',
    'BufferError',
    'EOFError',
    'ImportError',
    # 'ModuleNotFoundError',
    'IndexError',
    'KeyError',
    'UnboundLocalError',
    'BlockingIOError',
    'FileExistsError',
    'FileNotFoundError',
    'IsADirectoryError',
    'NotADirectoryError',
    'ReferenceError',
    'NotImplementedError',
    'SyntaxError',
    'SystemError',
    'TypeError',
    'ValueError',
    'UnicodeDecodeError',
    'UnicodeEncodeError',
    'UnicodeTranslateError',
]