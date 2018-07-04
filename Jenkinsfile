pipeline {
    agent any
    tools {
        maven 'Maven 3'
        jdk 'jdk8'
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

        stage('Prepare') {
            
        }

        stage ('Build') {
            echo 'Building..'
            steps {
                sh 'mvn clean package'
            }
            post {
                success {
                    junit 'target/surefire-reports/**/*.xml'
                }
            }
        }

        stage('Test') {
            echo 'Testing..'
            steps {
                sh 'mvn test'
            }
        }

        stage ('Deploy') {
            echo 'Deploying..'
          
        }

    }
}
