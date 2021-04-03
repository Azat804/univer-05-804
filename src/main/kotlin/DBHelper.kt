import java.sql.*
import java.io.BufferedReader
import java.io.FileReader

class DBHelper(
    val dbName: String,
    val host: String = "localhost", val port: Int = 3306, val user: String = "root", val password: String = "root"
) {
    private var stmt: Statement? = null
    private  val filePath: List<String>
        get() = listOf(
            "academic_performance.csv",
            "department.csv",
            "direction.csv",
            "qualification.csv",
            "acad_group.csv",
            "subject.csv",
            "student.csv",
            "curriculum.csv",
            "curriculum_subject.csv"
        )

    private fun connect() {
        stmt?.run {
            if (!isClosed) close()
        }
        var rep = 0
        do {
            try {
                stmt =
                    DriverManager.getConnection("jdbc:mysql://$host:$port/$dbName?serverTimezone=UTC", user, password)
                        .createStatement()
            } catch (e: SQLSyntaxErrorException) {
                val tstmt =
                    DriverManager.getConnection("jdbc:mysql://$host:$port/?serverTimezone=UTC", user, password)
                        .createStatement()
                tstmt.execute("CREATE SCHEMA `$dbName`")
                tstmt.closeOnCompletion()
                rep++
            }
        } while (stmt == null && rep < 2)
    }

    private fun disconnect() {
        stmt?.close()
    }

    fun createDatabase() {
        connect()
        dropTables()
        createTables()
        fillTables()
        sql()
        disconnect()
    }

    private fun dropTables() {
        stmt?.run {
            addBatch("DROP TABLE IF EXISTS `academic_performance`")
            addBatch("DROP TABLE IF EXISTS `curriculum_subject`")
            addBatch("DROP TABLE IF EXISTS `student`")
            addBatch("DROP TABLE IF EXISTS `acad_group`")
            addBatch("DROP TABLE IF EXISTS `qualification`")
            addBatch("DROP TABLE IF EXISTS `curriculum`")
            addBatch("DROP TABLE IF EXISTS `direction`")
            addBatch("DROP TABLE IF EXISTS `subject`")
            addBatch("DROP TABLE IF EXISTS `department`")
            executeBatch()
        }
    }

    private fun createTables() {
        stmt?.run {
            addBatch("START TRANSACTION;")
            addBatch(
                "CREATE TABLE `student` (\n" +
                        "  `ID` int NOT NULL PRIMARY KEY AUTO_INCREMENT,\n" +
                        "  `Lastname` VARCHAR(40) NOT NULL,\n" +
                        "  `Firstname` VARCHAR(40) NOT NULL,\n" +
                        "  `Middlename` VARCHAR(40) DEFAULT NULL,\n" +
                        "  `Group_id` VARCHAR(6) NOT NULL,\n" +
                        "  `Gender` set('М','Ж') NOT NULL,\n" +
                        "  `Birth` date NOT NULL\n" +
                        ");"
            )
            addBatch("ALTER TABLE `student`\n" +
                    "  ADD KEY `Name` (`Lastname`,`Firstname`,`Middlename`),\n" +
                    "  ADD KEY `Group_id` (`Group_id`);")
            addBatch("CREATE TABLE  if not exists `acad_group`  ( `Group_id` VARCHAR(6) NOT NULL, `Curriculum_id` INT(2) NOT NULL , `Qualification_id` INT(3) NOT NULL , PRIMARY KEY (`Group_id`))")

            addBatch(
                "ALTER TABLE `student`\n" +
                        "  ADD CONSTRAINT `student_ibfk_1` FOREIGN KEY (`Group_id`) REFERENCES `acad_group` (`Group_id`) ON DELETE RESTRICT ON UPDATE CASCADE;"
            )
            addBatch("CREATE TABLE if not exists `qualification` ( `Qualification_id` INT(3) NOT NULL AUTO_INCREMENT PRIMARY KEY , `Title` set('Бакалавриат','Магистратура','Специалитет') NOT NULL  DEFAULT 'Бакалавриат')")
            addBatch("CREATE TABLE if not exists `direction` ( `Direction_id` VARCHAR(15) NOT NULL  , `Title` VARCHAR(40) NOT NULL , PRIMARY KEY (`Direction_id`))")
            addBatch("CREATE TABLE if not exists `curriculum` ( `Curriculum_id` INT(2) NOT NULL AUTO_INCREMENT , `beginning_year_education` YEAR NOT NULL , `Direction_id` VARCHAR(15) NOT NULL, PRIMARY KEY (`Curriculum_id`))")
            addBatch(
                "ALTER TABLE `curriculum`\n" +
                        "  ADD CONSTRAINT `curriculum_ibfk_1` FOREIGN KEY (`Direction_id`) REFERENCES `direction` (`Direction_id`) ON DELETE RESTRICT ON UPDATE CASCADE;"
            )
            addBatch("CREATE TABLE if not exists `department` ( `Department_id` INT(15) NOT NULL AUTO_INCREMENT , `Title` VARCHAR(60) NOT NULL , PRIMARY KEY (`Department_id`))")
            addBatch("CREATE TABLE if not exists`subject` ( `Subject_code` VARCHAR(50) NOT NULL , `Title` VARCHAR(100) NOT NULL , `Department_id` INT(15) NOT NULL, PRIMARY KEY (`Subject_code`))")
            addBatch(
                "ALTER TABLE `subject`\n" +
                        "  ADD CONSTRAINT `subject_ibfk_1` FOREIGN KEY (`Department_id`) REFERENCES `department` (`Department_id`) ON DELETE RESTRICT ON UPDATE CASCADE;"
            )
            addBatch("CREATE TABLE if not exists `curriculum_subject` ( `Subject_id` INT(75) NOT NULL AUTO_INCREMENT  , `Curriculum_id` INT(2) NOT NULL , `Subject_code` VARCHAR(20) NOT NULL , `Semestr` INT(12) NOT NULL, `Hour` INT(200) NOT NULL, `Reporting_form` SET('Экзамен', 'Зачет', 'Диф.зачет')  NOT NULL, PRIMARY KEY (`Subject_id`))")
            addBatch(
                "ALTER TABLE `curriculum_subject`\n" +
                        "  ADD CONSTRAINT `curriculum_subject_ibfk_1` FOREIGN KEY (`Subject_code`) REFERENCES `subject` (`Subject_code`) ON DELETE RESTRICT ON UPDATE CASCADE;"
            )
            addBatch("ALTER TABLE  `curriculum_subject`  ADD CONSTRAINT `curriculum_subject_ibfk_2` FOREIGN KEY (`Curriculum_id`) REFERENCES `curriculum` (`Curriculum_id`) ON DELETE RESTRICT ON UPDATE CASCADE;")
            addBatch("CREATE TABLE if not exists `academic_performance` ( `Student_id` INT NOT NULL  , `Subject_id` INT(75) NOT NULL, `Score` INT(100) NOT NULL DEFAULT '56' , `Attempt` INT(3) NOT NULL DEFAULT '1' , PRIMARY KEY (`Student_id`, `Subject_id`))")
            addBatch(
                "ALTER TABLE `academic_performance`\n" +
                        "  ADD CONSTRAINT `academic_performance_ibfk_1` FOREIGN KEY (`Student_id`) REFERENCES `student` (`ID`) ON DELETE RESTRICT ON UPDATE CASCADE;"
            )
            addBatch("ALTER TABLE  `academic_performance`  ADD CONSTRAINT `academic_performance_ibfk_2` FOREIGN KEY (`Subject_id`) REFERENCES `curriculum_subject` (`Subject_id`) ON DELETE RESTRICT ON UPDATE CASCADE;")
            addBatch("COMMIT;")
            executeBatch()
        }
    }

   private fun fillTables() {
        val academicPerformance: BufferedReader?
        val department: BufferedReader?
        val direction: BufferedReader?
        val qualification: BufferedReader?
        val academicGroup: BufferedReader?
        val subject: BufferedReader?
        val student: BufferedReader?
        val curriculum: BufferedReader?
        val curriculumSubject: BufferedReader?
        academicPerformance = BufferedReader(FileReader(filePath[0]))
        academicPerformance.readLine()
        var line = academicPerformance.readLine()

        department = BufferedReader(FileReader(filePath[1]))
        department.readLine()
        var line2 = department.readLine()

        direction = BufferedReader(FileReader(filePath[2]))
        direction.readLine()
        var line3 = direction.readLine()

        qualification = BufferedReader(FileReader(filePath[3]))
        qualification.readLine()
        var line4 = qualification.readLine()

        academicGroup = BufferedReader(FileReader(filePath[4]))
        academicGroup.readLine()
        var line5 = academicGroup.readLine()

        subject = BufferedReader(FileReader(filePath[5]))
        subject.readLine()
        var line6 = subject.readLine()

        student = BufferedReader(FileReader(filePath[6]))
        student.readLine()
        var line7 = student.readLine()

        curriculum = BufferedReader(FileReader(filePath[7]))
        curriculum.readLine()
        var line8 = curriculum.readLine()

        curriculumSubject = BufferedReader(FileReader(filePath[8]))
        curriculumSubject.readLine()
        var line9 = curriculumSubject.readLine()

        stmt?.run {
            addBatch("delete from `academic_performance`")
            addBatch("delete from `department`")
            addBatch("delete from `direction`")
            addBatch("delete from `qualification`")
            addBatch("delete from `acad_group`")
            addBatch("delete from `subject`")
            addBatch("delete from `student`")
            addBatch("delete from `curriculum`")
            addBatch("delete from `curriculum_subject`")

            while (line2 != null) {
                val tokens2 = line2.split(";")
                val token = tokens2[1]
                         addBatch("INSERT INTO `department` (`Title`) VALUES ('$token')")
                line2 = department.readLine()
            }
            while (line3 != null) {
                val tokens3 = line3.split(";")
                val token1 = tokens3[0]
                val token2 = tokens3[1]
                addBatch("INSERT INTO `direction` (`Direction_id`,`Title`) VALUES ('$token1','$token2')")
                line3 = direction.readLine()
            }
            while (line4 != null) {
                val tokens4 = line4.split(";")
                val token4 = tokens4[1]
                addBatch("INSERT INTO `qualification` (`Title`) VALUES ('$token4')")
                line4 = qualification.readLine()
            }
            while (line5 != null) {
                val tokens5 = line5.split(";")
                val token6 = tokens5[0]
                val token7 = tokens5[1]
                val token8 = tokens5[2]
                addBatch("INSERT INTO `acad_group` (`Group_id`,`Curriculum_id`,`Qualification_id`) VALUES ('$token6','$token7','$token8')")
                line5 = academicGroup.readLine()
            }
            while (line6 != null) {
                val tokens6 = line6.split(";")
                val token9 = tokens6[0]
                val token10 = tokens6[1]
                val token11 = tokens6[2]
                addBatch("INSERT INTO `subject` (`Subject_code`,`Title`,`Department_id`) VALUES ('$token9','$token10','$token11')")
                line6 = subject.readLine()
            }
            while (line7 != null) {
                val tokens7 = line7.split(";")
                val token12 = tokens7[1]
                val token13 = tokens7[2]
                val token14 = tokens7[3]
                val token15 = tokens7[4]
                val token16 = tokens7[5]
                val token17 = tokens7[6]
                addBatch("INSERT INTO `student` (`Lastname`,`Firstname`,`Middlename`, `Group_id`,`Gender`, `Birth`) VALUES ('$token12','$token13','$token14', '$token15', '$token16', '$token17')")
                line7 = student.readLine()
            }
            while (line8 != null) {
                val tokens8 = line8.split(";")
                val token19 = tokens8[1]
                val token20 = tokens8[2]
                addBatch("INSERT INTO `curriculum` (`beginning_year_education`, `Direction_id`) VALUES ( '$token19','$token20')")
                line8 = curriculum.readLine()
            }
            while (line9 != null) {
                val tokens9 = line9.split(";")
                val token21 = tokens9[1]
                val token22 = tokens9[2]
                val token23 = tokens9[3]
                val token24 = tokens9[4]
                val token25 = tokens9[5]
                addBatch("INSERT INTO `curriculum_subject` (`Curriculum_id`, `Subject_code`, `Semestr`, `Hour`, `Reporting_form`) VALUES ( '$token21','$token22', '$token23', '$token24', '$token25')")
                line9 = curriculumSubject.readLine()
            }
            while (line != null) {
                val tokens = line.split(";")
                val token26 = tokens[0]
                val token27 = tokens[1]
                val token28 = tokens[2]
                val token29 = tokens[3]
                addBatch("INSERT INTO `academic_performance` (`Student_id`,`Subject_id`,`Score`,`Attempt`) VALUES ('$token26','$token27','$token28','$token29')")
                line = academicPerformance.readLine()
            }
            executeBatch()
        }

    }
    private fun sql() {
        val sql="SELECT  ID,Lastname, Firstname, Middlename, student.Group_id,MIN(CASE WHEN Attempt=1 AND Score BETWEEN 71 AND 85 THEN (SELECT 2100 as stip) WHEN Attempt=1 AND Score>=86 THEN (SELECT 3100 as stip3) ELSE (SELECT 0 as stip2) END) as stipend" +
                " FROM student,academic_performance, (SELECT * FROM curriculum_subject WHERE Reporting_form IN('Экзамен', 'Диф.зачет')) as cs, acad_group," +
                "   (SELECT (CASE WHEN YEAR(now())-beginning_year_education=1 AND MONTH(now()) BETWEEN 2 AND 6 THEN (SELECT 1 as sm1) WHEN YEAR(now())-beginning_year_education=1 AND MONTH(now())>6 THEN (SELECT 2 as sm2) WHEN YEAR(now())-beginning_year_education=2 AND MONTH(now()) BETWEEN 2 AND 6 THEN (SELECT 3 as sm3) WHEN YEAR(now())-beginning_year_education=2 AND MONTH(now())>6 THEN (SELECT 4 as sm4) WHEN YEAR(now())-beginning_year_education=3 AND MONTH(now()) BETWEEN 2 AND 6 THEN (SELECT 5 as sm5) WHEN YEAR(now())-beginning_year_education=3 AND MONTH(now())>6 THEN (SELECT 6 as sm6) WHEN YEAR(now())-beginning_year_education=4 AND MONTH(now()) BETWEEN 2 AND 6 THEN (SELECT 7 as sm7) WHEN YEAR(now())-beginning_year_education=4 AND MONTH(now())>6 THEN (SELECT 8 as sm8) END) as lastsem, curriculum.Curriculum_id FROM curriculum)  as filt" +
                "  WHERE filt.Curriculum_id=acad_group.Curriculum_id AND acad_group.Group_id=student.Group_id AND student.ID=academic_performance.Student_id AND   filt.lastsem=cs.Semestr AND academic_performance.Subject_id=cs.Subject_id GROUP BY ID ORDER BY Group_id, Lastname"
        val rs=stmt?.executeQuery(sql)

        while(rs?.next()==true) {
            for(i in 1..6) { print(rs.getString(i) + " ") }
            print("\n")
        }
    }
}