@echo off
timeout /t 2 >nul
del "main.py"
rename "main_download.py" "main.py"
del "replace_script.bat"
start "" "main.py"