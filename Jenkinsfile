pipeline{
    agent any
   
    stages{
       stage('GetCode'){
            steps{
                git 'https://github.com/vishnu491/pyspark.git'
            }
         }        
       
        stage('SonarQube analysis') {
//    def scannerHome = tool 'SonarScanner 4.0';
        steps{
        withSonarQubeEnv('sonarqube-9.9') { 
        // If you have configured more than one global server connection, you can specify its name
//      sh "${scannerHome}/bin/sonar-scanner"
        sh "mvn sonar:sonar"
    }
        }
        }
       
    }
}
