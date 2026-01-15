/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.launcher.env.environment;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment.Builder;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeExasol
        extends EnvironmentProvider
{
    private static final int EXASOL_PORT = 8563;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeExasol(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-exasol/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("exasol", forHostPath(configDir.getPath("exasol.properties")));
        builder.addContainer(createExasol());
        configureTempto(builder, configDir);
    }

    public static WaitStrategy exasolHttpReady(String path, int port) {
        return Wait.forHttp(path)
                .forPort(port)                 // if the HTTP service is NOT on the first exposed port
                //.usingTls()                    // because your script calls https://...
                .allowInsecure()               // because your script uses curl -k (skip cert validation)
                .withReadTimeout(java.time.Duration.ofSeconds(5))  // like curl --max-time 5 (rough equivalent)
                .forResponsePredicate(body ->
                        body != null
                                && !body.isEmpty()
                                && (body.contains("status")
                                || body.contains("WebSocket")
                                || body.contains("error")))
                .withStartupTimeout(java.time.Duration.ofMinutes(5)); // overall “give up” time (replace infinite loop)
    }

    public static WaitStrategy exasolReadyViaCurl(int port) {
        String command =
                "bash -c '"
                        + "while true; do "
                        + "  resp=$(curl -sk --max-time 5 https://localhost:" + port + "/ 2>/dev/null || true); "
                        + "  if [ -n \"$resp\" ] && "
                        + "     (echo \"$resp\" | grep -q \"status\" || "
                        + "      echo \"$resp\" | grep -q \"WebSocket\" || "
                        + "      echo \"$resp\" | grep -q \"error\"); then "
                        + "    exit 0; "
                        + "  fi; "
                        + "  sleep 0.5; "
                        + "done'";

        return Wait.forSuccessfulCommand(command)
                .withStartupTimeout(java.time.Duration.ofMinutes(5));
    }

    public static WaitStrategy waitForExasolStage6Finished()
    {
        return new LogMessageWaitStrategy()
                .withRegEx("(?s).*stage6\\s*:\\s*All\\s+stages\\s+finished.*")
                .withTimes(1);
    }

    WaitAllStrategy ready = new WaitAllStrategy(WaitAllStrategy.Mode.WITH_INDIVIDUAL_TIMEOUTS_ONLY)
            .withStrategy(waitForExasolStage6Finished().withStartupTimeout(java.time.Duration.ofSeconds(10)))
            .withStrategy(forSelectedPorts(EXASOL_PORT).withStartupTimeout(java.time.Duration.ofSeconds(10)));


    private DockerContainer createExasol()
    {
        DockerContainer container = new DockerContainer("exadockerci4/docker-db:2025.1.8_dev_java_slc_only", "exasol") //Test container tailored to reduce used disk space and solve CI disk space pressure issue.
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy().withTimeout(java.time.Duration.ofSeconds(10)))
                //.waitingFor(forSelectedPorts(EXASOL_PORT).withStartupTimeout(java.time.Duration.ofSeconds(10)))
                //.waitingFor(ready)
                //.waitingFor(exasolHttpReady("/", EXASOL_PORT))
                .waitingFor(exasolReadyViaCurl(EXASOL_PORT))//;
                .withEnv("COSLWD_ENABLED", "1"); //Disables rsyslogd, cleans up log clutter and speeds up database startup
                //.withStartupAttempts(3);
        container.setPrivilegedMode(true);
        portBinder.exposePort(container, EXASOL_PORT);
        return container;
    }
}
