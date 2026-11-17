# Python 3.11: libsql ships no prebuilt win_amd64 wheel for newer interpreters (3.14 falls
# back to a Rust source build that needs the MSVC linker). Use the py launcher, not bare python.
Invoke-Expression "py -3.11 -m venv venv"
