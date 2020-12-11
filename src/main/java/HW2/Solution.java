package HW2;

import HW2.business.*;
import HW2.data.DBConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import HW2.data.PostgreSQLErrorCodes;

import java.util.ArrayList;

import static HW2.business.ReturnValue.*;


public class Solution {

    //Tables
    private static final String TESTS = "Tests";
    private static final String STUDENTS = "Students";
    private static final String SUPERVISORS = "Supervisors";
    //Relations
    private static final String ATTENDS = "Attends";
    private static final String OVERSEES = "Oversees";
    //Views
    private static final String SUPERVISOR_OVERSEES = "supervisor_oversees";
    //Types
    private static final String TEXT = "Text";
    private static final String INTEGER = "Integer";
    //PostgreSQL query types
    private static final String EXECUTE = "execute";
    private static final String EXECUTE_QUERY = "executeQuery";
    private static final String EXECUTE_UPDATE = "executeUpdate";
    

    private static Boolean compareSQLExceptions(SQLException e, PostgreSQLErrorCodes error) {
        int e_val = Integer.valueOf(e.getSQLState());  
        /* for debugging
        System.out.println("e_val is " + e_val);
        System.out.println("NOT_NULL_VIOLATION IS " + PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue());
        System.out.println("FOREIGN_KEY_VIOLATION IS " + PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue());
        System.out.println("UNIQUE_VIOLATION IS " + PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue());
        System.out.println("CHECK_VIOLATION IS " + PostgreSQLErrorCodes.CHECK_VIOLATION.getValue());
        */
        if (error == PostgreSQLErrorCodes.NOT_NULL_VIOLATION) {
            return e_val == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue();    
        } else if (error == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION) {
            return e_val == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue();    
        } else if (error == PostgreSQLErrorCodes.UNIQUE_VIOLATION) {
            return e_val == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue();    
        } else if (error == PostgreSQLErrorCodes.CHECK_VIOLATION) {
            return e_val == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue();    
        } 
        return false;
    } 

    private static Object executeStatementInDB(String statement, String query_type) throws SQLException {
        System.out.println(statement);
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(statement);
            if (query_type == EXECUTE) {
                return pstmt.execute();
            } else if (query_type == EXECUTE_QUERY) {
                return pstmt.executeQuery();
            } else if (query_type == EXECUTE_UPDATE) {
                return pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            try {
                if (pstmt != null)
                    pstmt.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    private static void createView(String name, String description) {
        try {
            executeStatementInDB("CREATE VIEW " + name + " AS " + description, EXECUTE);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void dropView(String name) {
        try {
            executeStatementInDB("DROP VIEW " + name, EXECUTE);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(String table) {
        String statement = "";
        if (table == TESTS) {
            statement = getTestsTableStatement();
        } else if (table == STUDENTS) {
            statement = getStudentsTableStatement();
        } else if (table == SUPERVISORS) {
            statement = getSupervisorsTableStatement();
        }  else if (table == ATTENDS) {
            statement = getAttendsTableStatement();
        } else if (table == OVERSEES) {
            statement = getOverseesTableStatement();
        }
        try { 
            executeStatementInDB(statement, EXECUTE);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }

    private static void clearTable(String table) {
        try { 
            executeStatementInDB("DELETE FROM " + table, EXECUTE);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dropTable(String table) {
        try { 
            executeStatementInDB("DROP TABLE IF EXISTS " + table, EXECUTE);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getTestsTableStatement() {
        return "CREATE TABLE " + TESTS + " ("               +
                    "ID INTEGER NOT NULL,\n"                +
                    "Semester INTEGER NOT NULL,\n"          +
                    "Time INTEGER NOT NULL,\n"              +
                    "Room INTEGER NOT NULL,\n"              +
                    "Day INTEGER NOT NULL,\n"               +
                    "CreditPoints INTEGER NOT NULL,\n"      +
                    "PRIMARY KEY(ID, SEMESTER),\n"          +
                    "CHECK(ID>0),\n"                        +
                    "CHECK(CreditPoints>0),\n"              +
                    "CHECK(Room>0),\n"                      +
                    "CHECK(Semester>=1 AND Semester<=3),\n" +
                    "CHECK(Time>=1 AND Time<=3),\n"         +
                    "CHECK(Day>=1 AND Day<=31),\n"          +
                    "UNIQUE(ID, Semester)"                  +
                ")";
    }

    private static String getStudentsTableStatement() {
        return "CREATE TABLE " + STUDENTS + " ("          +
                    "ID INTEGER NOT NULL,\n"              +
                    "Name TEXT NOT NULL,\n"               +
                    "Faculty TEXT NOT NULL,\n"            +
                    "CreditPoints INTEGER NOT NULL,\n"    +
                    "PRIMARY KEY(ID),\n"                  +
                    "CHECK(ID>0),\n"                      +
                    "CHECK(CreditPoints>=0),\n"           +
                    "UNIQUE(ID)"                          +
                ")";
    }

    private static String getSupervisorsTableStatement() {
        return "CREATE TABLE " + SUPERVISORS + " ("       +
                    "ID INTEGER NOT NULL,\n"              +
                    "Name TEXT NOT NULL,\n"               +
                    "Salary INTEGER NOT NULL,\n"          +
                    "PRIMARY KEY(ID),\n"                  +
                    "CHECK(ID>0),\n"                      +
                    "CHECK(Salary>=0),\n"                 +
                    "UNIQUE(ID)"                          +
                ")";
    }

    private static String getAttendsTableStatement() {
        return "CREATE TABLE " + ATTENDS + " ("                                         +
                    "TestID INTEGER NOT NULL,\n"                                        +
                    "Semester INTEGER NOT NULL,\n"                                      +
                    "StudentID INTEGER NOT NULL,\n"                                     +            
                    "CHECK(TestID>0),\n"                                                +
                    "CHECK(Semester>0),\n"                                              +
                    "CHECK(StudentID>0),\n"                                             +
                    "CONSTRAINT fk_tests\n"                                             +
                    "FOREIGN KEY (TestID, Semester) REFERENCES "                        +
                    TESTS + "(ID, Semester),\n"                                         +
                    "CONSTRAINT fk_student\n"                                           +
                    "FOREIGN KEY (StudentID) REFERENCES " + STUDENTS + "(ID),\n"        +
                    "UNIQUE(TestID, Semester, StudentID)"                               +
                ")";
    }

    private static String getOverseesTableStatement() {
        return "CREATE TABLE " + OVERSEES + " ("                                        +
                    "TestID INTEGER NOT NULL,\n"                                        +
                    "Semester INTEGER NOT NULL,\n"                                      +
                    "SupervisorID INTEGER NOT NULL,\n"                                  +
                    "CHECK(TestID>0),\n"                                                +
                    "CHECK(Semester>0),\n"                                              +
                    "CHECK(SupervisorID>0),\n"                                          +
                    "CONSTRAINT fk_tests\n"                                             +
                    "FOREIGN KEY (TestID, Semester) REFERENCES "                        +
                    TESTS + "(ID, Semester),\n"                                         +
                    "CONSTRAINT fk_supervisor\n"                                        +
                    "FOREIGN KEY (SupervisorID) REFERENCES " + SUPERVISORS + "(ID),\n"  +
                    "UNIQUE(TestID, Semester, SupervisorID)"                            +
                ")";
    }

    private static String prepareAddStatement(String table, Object[] attributes, Object[] values, Object[] value_types) {
        String statement = "INSERT INTO " + table + " (";
        for (int i=0; i<attributes.length; i++) {
            statement += attributes[i];
            if (i != attributes.length - 1) {
                statement += ", ";
            }
        }
        statement += ") ";
        statement += "VALUES (";
        for (int i=0; i<values.length; i++) {
            if (value_types[i] == TEXT) {
                statement += "\'";
            }
            statement += values[i];
            if (value_types[i] == TEXT) {
                statement += "\'";
            }
            if (i != attributes.length - 1) {
                statement += ", ";
            }
        }
        statement += ")";

        return statement;
    }

    private static ReturnValue addToTable(String table, Object[] attributes, Object[] values, Object[] value_types) {
        String statement = prepareAddStatement(table, attributes, values, value_types);
        try {
            executeStatementInDB(statement, EXECUTE);
        } catch (SQLException e) {
            if (compareSQLExceptions(e, PostgreSQLErrorCodes.NOT_NULL_VIOLATION)) {
                return BAD_PARAMS;
            } else if (compareSQLExceptions(e, PostgreSQLErrorCodes.CHECK_VIOLATION)) {
                return BAD_PARAMS;
            } else if (compareSQLExceptions(e, PostgreSQLErrorCodes.UNIQUE_VIOLATION)) {
                return ALREADY_EXISTS;
            } else if (compareSQLExceptions(e, PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION)) {
                return NOT_EXISTS;
            } else {
                return ERROR;
            }
            //TODO: complete for other error types
        }
        return OK;
    }

    public static void createTables() {
        InitialState.createInitialState();
        createTable(STUDENTS);
        createTable(SUPERVISORS);
        createTable(TESTS);
        createTable(ATTENDS);
        createTable(OVERSEES);
        createView(SUPERVISOR_OVERSEES, "SELECT * FROM supervisors S FULL OUTER JOIN oversees O ON S.id = O.supervisorid");
    } 

    public static void clearTables() {
        clearTable(ATTENDS);
        clearTable(OVERSEES);
        clearTable(TESTS);
        clearTable(STUDENTS);
        clearTable(SUPERVISORS);
    }

    public static void dropTables() {
        InitialState.dropInitialState();
        dropView(SUPERVISOR_OVERSEES);
        dropTable(ATTENDS);
        dropTable(OVERSEES);
        dropTable(TESTS);
        dropTable(STUDENTS);
        dropTable(SUPERVISORS);
    }

    public static ReturnValue addTest(Test test) {
        String[] attributes = {"ID", "Semester", "Time", "Room", "Day", "CreditPoints"};
        Object[] values = {test.getId(), test.getSemester(), test.getTime(), test.getRoom(), test.getDay(), test.getCreditPoints()};
        Object[] value_types = {INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER};
        ReturnValue retval = addToTable(TESTS, attributes, values, value_types);
        return retval;
    }

    private static String prepareSelectStatement(String table, Object[] attributes_to_select, Object[] attributes_for_where, Object[] values_for_where, Object[] attributes_for_group_by){
        String select_string = "";
        String where_string = "";
        String group_by_string = "";

        // Preparing select part
        for (int i = 0; i < attributes_to_select.length; i++) {
            if (i == 0) {
                select_string += "SELECT ";
            }
            select_string += attributes_to_select[i];
            if (i != attributes_to_select.length - 1) {
                select_string += ", ";
            }
        }
        
        // Preparing where part
        for (int i = 0; i < attributes_for_where.length; i++) {
            if (i == 0) {
                where_string += "\nWHERE ";
            }
            where_string += attributes_for_where[i];
            where_string += " = ";
            where_string += values_for_where[i]; 
            if (i != attributes_for_where.length - 1) {
                where_string += " AND ";
            }
        }

        // Preparing group by part
        for (int i = 0; i < attributes_for_group_by.length; i++) {
            if (i == 0) {
                group_by_string += "\nGROUP BY ";
            }
            group_by_string += attributes_for_group_by[i];
            if (i != attributes_for_group_by.length - 1) {
                group_by_string += ", ";
            }
        }

        String statement = select_string + " FROM " + table + where_string + group_by_string;
        return statement;
    }

    private static ResultSet selectFromDB(String table, Object[] attributes_to_select, Object[] attributes_for_where, Object[] values_for_where, Object[] attributes_for_group_by) {
        ResultSet rs = null;
        String statement = prepareSelectStatement(table, attributes_to_select, attributes_for_where, values_for_where, attributes_for_group_by);
        try {
            rs = (ResultSet)executeStatementInDB(statement, EXECUTE_QUERY);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs; 
    }

    public static Test getTestFromResultSet(ResultSet rs) throws SQLException {
        Test test = new Test();
        try {
            test.setId(rs.getInt(1));
            test.setSemester(rs.getInt(2));
            test.setTime(rs.getInt(3));
            test.setRoom(rs.getInt(4));
            test.setDay(rs.getInt(5));
            test.setCreditPoints(rs.getInt(6));
            rs.close();
        } catch (SQLException e) {
            throw e;
        }
        return test;
    }

    public static Test getTestProfile(Integer testID, Integer semester) {
        Test test;
        try {
            ResultSet rs = selectFromDB(TESTS, new Object[] {"*"}, new Object[] {"ID", "Semester"}, new Object[] {testID, semester}, new Object[] {});
            if (rs.next() == false) {
                return Test.badTest();
            }
            test = getTestFromResultSet(rs);
        } catch (SQLException e) {
            return Test.badTest();
        }
        return test;
    }

    private static String prepareDeleteStatement(String table, Object[] keys, Object[] values) {
        String statement = "DELETE FROM " + table;
        for (int i = 0; i < keys.length; i++) {
                if (i == 0) {
                    statement += "\nWHERE ";
                }
                statement += keys[i];
                statement += " = ";
                statement += values[i]; 
                if (i != keys.length - 1) {
                    statement += " AND ";
                }
            }
        return statement;
    }

    private static int deleteFromTable(String table, Object[] keys, Object[] values) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        String statement = prepareDeleteStatement(table, keys, values);
        int affectedRows = 0;
        try {
            affectedRows = (int)executeStatementInDB(statement, EXECUTE_UPDATE);
        } catch (SQLException e) {
            return -1;
        }
        return affectedRows;
    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
        int affectedRows = deleteFromTable(TESTS, new Object[] {"ID", "Semester"}, new Object[] {testID, semester});
        if (affectedRows == 0) {
            return NOT_EXISTS;
        } else if (affectedRows == -1 ) {
            return ERROR;
        }
		return OK;
    }

    public static ReturnValue addStudent(Student student) {
        String[] attributes = {"ID", "Name", "Faculty", "CreditPoints"};
        Object[] values = {student.getId(),student.getName(),student.getFaculty(),student.getCreditPoints()};
        Object[] value_types = {INTEGER, TEXT, TEXT, INTEGER};
        ReturnValue retval = addToTable(STUDENTS, attributes, values, value_types);
        return retval;
    }

    public static Student getStudentFromResultSet(ResultSet rs) throws SQLException {
        Student student = new Student();
        try {
            student.setId(rs.getInt(1));
            student.setName(rs.getString(2));
            student.setFaculty(rs.getString(3));
            student.setCreditPoints(rs.getInt(4));
            rs.close();
        } catch (SQLException e) {
            throw e;
        }
        return student;
    }

    public static Student getStudentProfile(Integer studentID) {
        Student student;
        try {
            ResultSet rs = selectFromDB(STUDENTS, new Object[] {"*"}, new Object[] {"ID"}, new Object[] {studentID}, new Object[] {});
            if (rs.next() == false) {
                return Student.badStudent();
            }
            student = getStudentFromResultSet(rs);
        } catch (SQLException e) {
            return Student.badStudent();
        }
        return student;
    }

    public static ReturnValue deleteStudent(Integer studentID) {
        int affectedRows = deleteFromTable(STUDENTS, new Object[] {"ID"}, new Object[] {studentID});
        if (affectedRows == 0) {
            return NOT_EXISTS;
        } else if (affectedRows == -1 ) {
            return ERROR;
        }
		return OK;
    }

    public static ReturnValue addSupervisor(Supervisor supervisor) {
        String[] attributes = {"ID", "Name", "Salary"};
        Object[] values = {supervisor.getId(),supervisor.getName(),supervisor.getSalary()};
        Object[] value_types = {INTEGER, TEXT, INTEGER};
        ReturnValue retval = addToTable(SUPERVISORS, attributes, values, value_types);
        return retval;
    }

    public static Supervisor getSupervisorFromResultSet(ResultSet rs) throws SQLException {
        Supervisor supervisor = new Supervisor();
        try {
            supervisor.setId(rs.getInt(1));
            supervisor.setName(rs.getString(2));
            supervisor.setSalary(rs.getInt(3));
            rs.close();
        } catch (SQLException e) {
            throw e;
        }
        return supervisor;
    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        Supervisor supervisor;
        try {
            ResultSet rs = selectFromDB(SUPERVISORS, new Object[] {"*"}, new Object[] {"ID"}, new Object[] {supervisorID}, new Object[] {});
            if (rs.next() == false) {
                return Supervisor.badSupervisor();
            }
            supervisor = getSupervisorFromResultSet(rs);
        } catch (SQLException e) {
            return Supervisor.badSupervisor();
        }
        return supervisor;
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        int affectedRows = deleteFromTable(SUPERVISORS, new Object[] {"ID"}, new Object[] {supervisorID});
        if (affectedRows == 0) {
            return NOT_EXISTS;
        } else if (affectedRows == -1 ) {
            return ERROR;
        }
		return OK;
    }

    public static ReturnValue studentAttendTest(Integer studentID, Integer testID, Integer semester) {
        String[] attributes = {"StudentID", "TestID", "Semester"};
        Object[] values = {studentID, testID, semester};
        Object[] value_types = {INTEGER, INTEGER, INTEGER};
        ReturnValue retval = addToTable(ATTENDS, attributes, values, value_types);
        return retval;
    }

    public static ReturnValue studentWaiveTest(Integer studentID, Integer testID, Integer semester) {
        int affectedRows = deleteFromTable(ATTENDS, new Object[] {"StudentID", "TestID", "Semester"}, 
                                                    new Object[] {studentID, testID, semester});
        if (affectedRows == 0) {
            return NOT_EXISTS;
        } else if (affectedRows == -1 ) {
            return ERROR;
        }
		return OK;
    }

    public static ReturnValue supervisorOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        String[] attributes = {"SupervisorID", "TestID", "Semester"};
        Object[] values = {supervisorID, testID, semester};
        Object[] value_types = {INTEGER, INTEGER, INTEGER};
        ReturnValue retval = addToTable(OVERSEES, attributes, values, value_types);
        return retval;
    }

    public static ReturnValue supervisorStopsOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
        int affectedRows = deleteFromTable(OVERSEES, new Object[] {"SupervisorID", "TestID", "Semester"}, 
                                                     new Object[] {supervisorID, testID, semester});
        if (affectedRows == 0) {
            return NOT_EXISTS;
        } else if (affectedRows == -1 ) {
            return ERROR;
        }
		return OK;
    }

    public static Float averageTestCost() {
        ResultSet rs;
        try {
            rs = selectFromDB(SUPERVISOR_OVERSEES, new Object[] {"id", "SUM(salary)"}, new Object[] {}, new Object[] {}, new Object[] {"id"});
            if (rs.next() == false) {
                //return Test.badTest();
            }
        } catch (SQLException e) {
            //return Test.badTest();
        }
        return 0.0f;
    }

    public static Integer getWage(Integer supervisorID) {
        return 0;
    }

    public static ArrayList<Integer> supervisorOverseeStudent() {
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> testsThisSemester(Integer semester) {
        return new ArrayList<Integer>();
    }

    public static Boolean studentHalfWayThere(Integer studentID) {
        return true;
    }

    public static Integer studentCreditPoints(Integer studentID) {
        return 0;
    }

    public static Integer getMostPopularTest(String faculty) {
        return 0;
    }

    public static ArrayList<Integer> getConflictingTests() {
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> graduateStudents() {
        return new ArrayList<Integer>();
    }

    public static ArrayList<Integer> getCloseStudents(Integer studentID) {
        return new ArrayList<Integer>();
    }
}

