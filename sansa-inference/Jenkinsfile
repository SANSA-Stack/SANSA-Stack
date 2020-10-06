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
            steps {
                echo 'Preparing..'
            }
        }

        stage ('Build') {
            steps {
                echo 'Building..'
                sh 'mvn clean package'
            }
        }

        stage('Test') {            
            steps {
                echo 'Testing..'
                sh 'mvn test'
            }
        }

        stage ('Deploy') {
            steps {
                echo 'Deploying..'
            }
          
        }

    }
}
