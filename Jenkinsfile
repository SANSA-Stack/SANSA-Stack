pipeline {
    agent any
    tools {
        maven 'Maven 3.3.9'
        jdk 'jdk8'
    }
    environment {
        BRANCH_NAME = 'develop'
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
                sh 'mvn -DskipTests clean deploy' 
            }
        }
    }
}
