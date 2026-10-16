/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
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

// Opt-in CI orchestration for the repository's existing Databricks build/test scripts.
Map run(script, dbUtils, Closure build, Closure test) {
    def preferred = script.env.DB_TYPE
    if (!(preferred in ['aws', 'azure3'])) {
        script.error 'Databricks fallback supports only aws/azure3 profiles'
    }
    if (!(script.env.DB_RUNTIME ==~ /[A-Za-z0-9][A-Za-z0-9_.-]*/) ||
        !(script.env.BUILD_NUMBER ==~ /[0-9]+/) || !script.env.BUILD_TAG ||
        !script.env.WORKSPACE || !script.env.NODE_NAME) {
        script.error 'Databricks fallback requires DB_RUNTIME, BUILD_NUMBER, BUILD_TAG, WORKSPACE and NODE_NAME'
    }
    // Keep per-attempt configuration outside the source tar sent to the cluster.
    script.sh 'mkdir -p .databricks-fallback'
    def alternate = preferred == 'aws' ? 'azure3' : 'aws'
    // Retry orchestration is shared with future CSPUTIL callers; this callback owns cuDF-Spark.
    // Rebuild on each cluster. Publish artifacts/trigger downstream jobs only after run returns.
    return script.databricksRetry(script, preferred, alternate) { target, attempt, finalAttempt ->
        def stateFile = "databricks-attempt-${script.env.BUILD_NUMBER}-${script.env.DB_RUNTIME}-${attempt}-${target}.json"
        def initDir = "${script.env.INIT_SCRIPTS_DIR}-${attempt}-${target}"
        def retryable = false
        def reason = ''
        script.withEnv([
            "DB_TYPE=$target",
            "DATABRICKS_HOST=${dbUtils.getHost(target)}",
            "DATABRICKS_DRIVER=${dbUtils.getDriver(target)}",
            "DATABRICKS_WORKER=${dbUtils.getWorker(target)}",
            "CLUSTER_NAME=CI-${script.env.BUILD_TAG}-${script.env.DB_RUNTIME}-${attempt}-${target}",
            "INIT_SCRIPTS_DIR=$initDir",
            // An Azure-preferred job must not inherit an AWS-specific zone override.
            'DB_ZONE=auto'
        ]) {
            script.withCredentials([
                script.string(credentialsId: dbUtils.getToken(target), variable: 'DATABRICKS_TOKEN'),
                script.file(credentialsId: 'SPARK_DATABRICKS_PRIVKEY', variable: 'DATABRICKS_PRIVKEY')
            ]) {
                def primaryError = null
                try {
                    def paths = script.env.INIT_SCRIPTS ?
                        dbUtils.uploadFiles(script, script.env.INIT_SCRIPTS, initDir) : ''
                    script.withEnv(["INIT_SCRIPT_PATHS=$paths"]) {
                        script.stage("Prepare ${script.env.DB_RUNTIME} DB ($target, attempt ${attempt + 1})") {
                            def configFile = ".databricks-fallback/config-${stateFile}"
                            script.writeJSON(file: configFile, json: script.readJSON(returnPojo: true,
                                text: script.sh(returnStdout: true,
                                    script: 'python3 jenkins/databricks/fallback-config.py')))
                            def status = script.sh(returnStatus: true,
                                script: "python3 jenkins/databricks/cluster_fallback.py prepare --state $stateFile --config $configFile")
                            def state = script.fileExists(stateFile) ?
                                script.readJSON(file: stateFile, returnPojo: true) : [:]
                            if (state.schema != 1 || state.workspace != script.env.DATABRICKS_HOST ||
                                state.jenkins_workspace != script.env.WORKSPACE ||
                                state.jenkins_node != script.env.NODE_NAME ||
                                state.build_tag != script.env.BUILD_TAG || state.runtime != script.env.DB_RUNTIME ||
                                state.profile != target) {
                                script.error 'Missing, incompatible or foreign Databricks attempt state'
                            }
                            reason = state.reason ?: 'UNKNOWN'
                            retryable = status == 75 && state.retryable == true
                            script.echo "Databricks preparation: exit=$status, reason=$reason, retryable=$retryable"
                            if (status == 0 && state.phase == 'READY' && state.cluster_id) {
                                script.echo "Databricks ${script.env.DB_RUNTIME}: using $target (attempt ${attempt + 1})"
                                // Compilation failures must never enter the startup classifier.
                                script.echo 'Databricks: starting build callback (build failures do not trigger fallback)'
                                build(state.cluster_id as String)
                                script.echo 'Databricks: build callback completed; installing Spark readiness hook'
                                script.sh "python3 jenkins/databricks/cluster_fallback.py install-readiness --state $stateFile"
                                def extraEnvs = script.env.EXTRA_ENVS ? "${script.env.EXTRA_ENVS}," : ''
                                script.withEnv(["EXTRA_ENVS=${extraEnvs}SPARK_STARTUP_CHECK=/home/ubuntu/.databricks-fallback/spark_readiness.sh"]) {
                                    try {
                                        script.echo 'Databricks: starting test callback with Spark readiness checks'
                                        test(state.cluster_id as String)
                                        script.echo 'Databricks: test callback completed; verifying readiness results'
                                    } catch (hudson.AbortException testError) {
                                        // Interruptions bypass this catch. Product failures have no failed readiness marker.
                                        script.echo 'Databricks: test callback failed; checking for a classified startup failure'
                                        try {
                                            def classification = script.sh(returnStatus: true,
                                                script: "python3 jenkins/databricks/cluster_fallback.py classify-startup --state $stateFile")
                                            state = script.readJSON(file: stateFile, returnPojo: true)
                                            retryable = classification == 75 && state.retryable == true
                                            reason = state.reason ?: 'UNKNOWN'
                                            script.echo "Databricks startup classification: exit=$classification, reason=$reason, retryable=$retryable"
                                        } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException interrupted) {
                                            throw interrupted
                                        } catch (Exception diagnosticError) {
                                            script.echo 'Startup classification unavailable; preserving the original failure'
                                            throw testError
                                        }
                                        if (!retryable || finalAttempt) {
                                            script.echo 'Databricks: no provider retry; propagating the test failure'
                                            throw testError
                                        }
                                    }
                                    if (!retryable) {
                                        script.sh "python3 jenkins/databricks/cluster_fallback.py verify-readiness --state $stateFile"
                                    }
                                }
                            } else if (!retryable || finalAttempt) {
                                script.error "Databricks preparation failed on $target: $reason"
                            }
                        }
                    }
                } catch (Exception error) {
                    primaryError = error
                    throw error
                } finally {
                    cleanupAttempt(script, dbUtils, stateFile, initDir, primaryError)
                }
            }
        }
        // Cleanup failures throw above; the coordinator must never allocate another cluster then.
        def cleanedState = script.readJSON(file: stateFile, returnPojo: true)
        return [retryable: retryable, reason: reason, cleanup_complete: cleanedState.cleanup_complete == true]

    }
}

void cleanupAttempt(script, dbUtils, String stateFile, String initDir, Exception primaryError) {
    def cleanupError = null
    script.echo "Databricks: cleaning attempt recorded in $stateFile"
    try {
        if (script.fileExists(stateFile)) {
            script.timeout(time: 5, unit: 'MINUTES') {
                script.retry(3) {
                    script.sh "python3 jenkins/databricks/cluster_fallback.py cleanup --state $stateFile"
                }
            }
        }
    } catch (Exception error) {
        cleanupError = error
        script.echo "Failed to terminate Databricks attempt; cluster handle is in $stateFile"
    }
    try {
        if (script.env.INIT_SCRIPTS) {
            script.echo 'Databricks: cleaning attempt init scripts'
            dbUtils.cleanUp(script, initDir)
        }
    } catch (Exception error) {
        if (!cleanupError || error instanceof org.jenkinsci.plugins.workflow.steps.FlowInterruptedException) {
            cleanupError = error
        }
        script.echo 'Failed to clean Databricks init scripts'
    }
    try {
        script.echo 'Databricks: archiving attempt state'
        script.archiveArtifacts(artifacts: stateFile, allowEmptyArchive: true)
    } catch (Exception error) {
        if (!cleanupError || error instanceof org.jenkinsci.plugins.workflow.steps.FlowInterruptedException) {
            cleanupError = error
        }
        script.echo 'Failed to archive Databricks diagnostics'
    }
    if (cleanupError && !primaryError) {
        throw cleanupError
    }
    script.echo(cleanupError ? 'Databricks: cleanup incomplete; preserving the primary failure' :
        'Databricks: attempt cleanup completed')
}

return this
