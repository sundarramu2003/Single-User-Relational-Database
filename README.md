Single-User-Database-Management-System
======================================
Detailed Instructions

1. Set up the development environment on Windows (32 Bit)

    We detail the steps of setting up a development environment in Eclipse on Windows. Linux and Mac users can follow the steps starting from "Set up JRE" if you prefer to develop on Eclipse. For an elaborated tutorial, please refer to this document Download.

    Set up MinGW

        Students should follow the standard C library and common C++ libaries, e.g. STL, to implement your project, and make sure your code is GCC/G++ compilable. Please make sure that you do not call any platform specific functions in your projects, such as Windows-specific APIs. If you are programming on Windows, you are recommended to use the tool chain from MinGW to build your project. An introduction to MinGW can be found at  http://www.mingw.org/.

        Download mingw-get-inst-20110802.exe from  http://sourceforge.net/projects/mingw/files/Automated%20MinGW%20Installer/mingw-get-inst/mingw-get-inst-20110802/. Run the installer. In the select components page, please check "C++ Compiler" and "MSYS Basic System" as well. Follow the default settings and finish the installation.

    Configure the Environment Variables

        Add the binary path of MinGW and MSYS to the Path variable, e.g. C:\Program Files\MinGW\bin and C:\Program Files\MinGW\msys\1.0\bin

    Set up JRE

        Make sure that JRE is available in your system so as to run Eclipse. If not, please follow  http://www.java.com/en/download/manual.jsp/ to download and install the JRE.

    Set up the Eclipse IDE for C/C++ Developers

        Download the zip file from  http://www.eclipse.org/downloads/download.php?file=/technology/epp/downloads/release/indigo/R/eclipse-cpp-indigo-incubation-win32.zip (on 32bit machine), unzip it and start Eclipse

    Create a new project in Eclipse

        Click "File"/"New"/"C++ Project" in Eclipse. You should be able to see "MinGW GCC" in the toolchains panel. Type "HelloWorld" in the project name, and click on "Finish". A default HelloWorld project will be created. Click the "Run" button in the toolbar, you should see the output in the console window.

    Try GDB in Eclipse

        GDB is included in the binary distribution of MinGW. Click the "Debug" button in the toolbar, and click "Yes" in the Perspective Switch dialog, and you will be now in the Debug Perspective. Press "F6" to step over each line of code until the last line of the code is reached. In case GDB is not included in your installation and the debugger is missing, please download and install the gdb-7.3-2-mingw32-bin.tar.lzma package at  http://sourceforge.net/projects/mingw/files/MinGW/BaseSystem/GDB/GDB-7.3/ and install it.


