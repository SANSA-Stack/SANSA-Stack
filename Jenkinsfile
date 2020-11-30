pipeline {
    agent any
    tools {
        maven 'Maven 3'
        jdk 'jdk8'
    }
    environment {
        BRANCH_NAME = 'develop'
        MAVEN_OPTS = '-Xmx1G -Xss256m'
    }
    stages {
        stage ('Initialize') {
            steps {
                sh '''
                    echo "PATH = ${PATH}"
                    echo "M2_HOME = ${M2_HOME}"
                '''
            }
        }

        stage ('Build and Push to Archiva') {
            steps {
                sh 'mvn --batch-mode -V -U -e -DskipTests clean deploy -pl :sansa-query-spark_2.12 -am' 
            }
        }
    }
}
