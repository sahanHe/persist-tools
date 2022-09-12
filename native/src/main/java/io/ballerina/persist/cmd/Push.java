/*
 *  Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.ballerina.persist.cmd;

import io.ballerina.cli.BLauncherCmd;
import io.ballerina.cli.utils.BuildTime;
import io.ballerina.persist.nodegenerator.SyntaxTreeGenerator;
import io.ballerina.persist.objects.BalException;
import io.ballerina.projects.JBallerinaBackend;
import io.ballerina.projects.JvmTarget;
import io.ballerina.projects.PackageCompilation;
import io.ballerina.projects.Project;
import io.ballerina.projects.ProjectEnvironmentBuilder;
import io.ballerina.projects.ProjectException;
import io.ballerina.projects.ProjectKind;
import io.ballerina.projects.directory.BuildProject;
import io.ballerina.projects.directory.ProjectLoader;
import io.ballerina.projects.internal.model.Target;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import static io.ballerina.cli.utils.FileUtils.getFileNameWithoutExtension;
import static io.ballerina.persist.PersistToolsConstants.COMPONENT_IDENTIFIER;
import static org.wso2.ballerinalang.compiler.util.ProjectDirConstants.BLANG_COMPILED_JAR_EXT;

/**
 * Class to implement "persist push" command for ballerina.
 *
 * @since 0.1.0
 */

@CommandLine.Command(
        name = "push",
        description = "Run SQL scripts.")

public class Push implements BLauncherCmd {

    private final PrintStream errStream = System.err;
    private static final String COMMAND_IDENTIFIER = "persist-push";
    public ProjectEnvironmentBuilder projectEnvironmentBuilder;
    Project balProject;
    public String sourcePath = "";
    public String configPath = "Config.toml";
    private String name = "";
    HashMap configurations;

    @CommandLine.Option(names = {"-h", "--help"}, hidden = true)
    private boolean helpFlag;

    @Override
    public void execute() {
        if (helpFlag) {
            String commandUsageInfo = BLauncherCmd.getCommandUsageInfo(COMMAND_IDENTIFIER);
            errStream.println(commandUsageInfo);
            return;
        }
        try  {
            if (projectEnvironmentBuilder == null) {
                balProject = ProjectLoader.loadProject(Paths.get(""));

            } else {
                balProject = ProjectLoader.loadProject(Paths.get(sourcePath), projectEnvironmentBuilder);
            }
            name = balProject.currentPackage().descriptor().org().value() + "." + balProject.currentPackage()
                    .descriptor().name().value();
        } catch (ProjectException e) {
            errStream.println("The current directory is not a Ballerina project!");
            return;
        }
        Target target = null;
        try {
            if (projectEnvironmentBuilder == null) {
                balProject = BuildProject.load(Paths.get("").toAbsolutePath());

            } else {
                balProject = BuildProject.load(projectEnvironmentBuilder, Paths.get(sourcePath).toAbsolutePath());
            }
            if (balProject.kind().equals(ProjectKind.BUILD_PROJECT)) {
                target = new Target(balProject.targetDir());
            } else {
                target = new Target(Files.createTempDirectory("ballerina-cache" + System.nanoTime()));
                target.setOutputPath(getExecutablePath(balProject));
            }
        } catch (IOException e) {
            errStream.println(e.getMessage());
            return;
        } catch (ProjectException e) {
            errStream.println(e.getMessage());
            return;
        }

        Path executablePath;
        try {
            executablePath = target.getExecutablePath(balProject.currentPackage()).toAbsolutePath().normalize();
        } catch (IOException e) {
            errStream.println(e.getMessage());
            return;
        }

        try {
            PackageCompilation pkgCompilation = balProject.currentPackage().getCompilation();
            JBallerinaBackend jBallerinaBackend = JBallerinaBackend.from(pkgCompilation, JvmTarget.JAVA_11);
            long start = 0;
            if (balProject.buildOptions().dumpBuildTime()) {
                start = System.currentTimeMillis();
            }
            jBallerinaBackend.emit(JBallerinaBackend.OutputType.EXEC, executablePath);
            if (balProject.buildOptions().dumpBuildTime()) {
                BuildTime.getInstance().emitArtifactDuration = System.currentTimeMillis() - start;
                BuildTime.getInstance().compile = false;
            }

            // Print warnings for conflicted jars
            if (!jBallerinaBackend.conflictedJars().isEmpty()) {
                errStream.println("\twarning: Detected conflicting jar files:");
                for (JBallerinaBackend.JarConflict conflict : jBallerinaBackend.conflictedJars()) {
                    errStream.println(conflict.getWarning(balProject.buildOptions().listConflictedClasses()));
                }
            }
        } catch (ProjectException e) {
            errStream.println(e.getMessage());
            return;
        }
        String sValue = new String();
        StringBuffer stringBuffer = new StringBuffer();
        configurations = new HashMap();
        String[] sqlLines = {};
        Connection connection;
        Statement statement;
        try {
            configurations = SyntaxTreeGenerator.readToml(
                    Paths.get(this.sourcePath, this.configPath), this.name);
            Path path = Paths.get(this.sourcePath, "SQL", "query.sql");
            FileReader fileReader = new FileReader(new File(path.toAbsolutePath().toString()));
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while ((sValue = bufferedReader.readLine()) != null) {
                stringBuffer.append(sValue);
            }
            bufferedReader.close();
            sqlLines = stringBuffer.toString().split(";");

        } catch (BalException e) {
            errStream.println(e.getMessage());
            return;
        } catch (IOException e) {
            errStream.println("Error occurred while reading generated SQL scripts!");
            return;
        }
        String url = String.format("jdbc:mysql://%s:%s",
                configurations.get("host").toString().replaceAll("\"", ""), configurations.get("port").toString());
        String user = configurations.get("user").toString().replaceAll("\"", "");
        String password = configurations.get("password").toString().replaceAll("\"", "");
        String database = configurations.get("database").toString().replaceAll("\"", "");
        try {
            connection = DriverManager.getConnection(url, user, password);
            ResultSet resultSet = connection.getMetaData().getCatalogs();
            boolean databaseExists = false;
            while (resultSet.next()) {

                if (resultSet.getString(1).trim().equals(database)) {
                    databaseExists = true;
                    break;
                }
            }
            if (!databaseExists) {
                statement = connection.createStatement();
                String query = String.format("CREATE DATABASE %s", database);
                statement.executeUpdate(query);
                errStream.println("Creating Database : " + database);
            }
            resultSet.close();
            connection.close();
            String databaseUrl = String.format("jdbc:mysql://%s:%s/%s",
                    configurations.get("host").toString().replaceAll("\"", ""), configurations.get("port").toString(),
                    configurations.get("database").toString().replaceAll("\"", ""));
            connection = DriverManager.getConnection(databaseUrl, user, password);
            statement = connection.createStatement();

            for (int line = 0; line < sqlLines.length; line++) {
                if (!sqlLines[line].trim().equals("")) {
                    statement.executeUpdate(sqlLines[line]);
                    errStream.println(">>" + sqlLines[line]);
                }
            }
        } catch (SQLException e) {
            errStream.println("*** Error : " + e.getMessage());
            errStream.println("*** ");
            return;
        }

    }

    private Path getExecutablePath(Project project) {

        Path fileName = project.sourceRoot().getFileName();
        Path currentDir = Paths.get(this.sourcePath);
        // If the --output flag is not set, create the executable in the current directory
        return currentDir.resolve(getFileNameWithoutExtension(fileName) + BLANG_COMPILED_JAR_EXT);

    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }
    public void setEnvironmentBuilder(ProjectEnvironmentBuilder projectEnvironmentBuilder) {
        this.projectEnvironmentBuilder = projectEnvironmentBuilder;
    }
    public HashMap getConfigurations() {
        return this.configurations;
    }

    @Override
    public void setParentCmdParser(CommandLine parentCmdParser) {
    }
    @Override
    public String getName() {
        return COMPONENT_IDENTIFIER;
    }
    
    @Override
    public void printLongDesc(StringBuilder out) {
        out.append("Generate database configurations file inside the Ballerina project").append(System.lineSeparator());
        out.append(System.lineSeparator());
    }
    
    @Override
    public void printUsage(StringBuilder stringBuilder) {
        stringBuilder.append("  ballerina " + COMPONENT_IDENTIFIER +
                " init").append(System.lineSeparator());
    }
}
