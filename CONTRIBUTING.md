# Contributing to Reactor Framework

## IDE Setup

### Eclipse
Take the following steps to work on the Reactor project with the Eclipse IDE.

 1. Generate the Eclipse project files via `./gradlew eclipse`.
 2. Import the project as an existing project via…

   1. the `File` entry in the toolbar,
   2. choose `Import`,
   3. choose `Existing Projects Into Workspace` and
   4. follow the wizard to import the Reactor projects into Eclipse.

 3. The Reactor project uses two source compliance levels: `1.7` for sources and `1.8` for tests. Unfortunately, Eclipse only supports one compliance level per project. In order to be able to work on the projects, you need to manually overwrite the compliance level to `1.8`. You can do this via…

   1. right click on a project,
   2. choose `Properties`,
   3. choose `Java Compiler`,
   4. set compiler compliance level to `1.8` and
   5. repeat for the remaining projects.
