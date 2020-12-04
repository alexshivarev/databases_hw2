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

    private static final String TESTS = "Tests";
    private static final String STUDENTS = "Students";
    private static final String SUPERVISORS = "Supervisors";
    private static final String TEXT = "Text";
    private static final String INTEGER = "Integer";

    private static Boolean compareSQLExceptions(SQLException e, PostgreSQLErrorCodes error) {
        int e_val = Integer.valueOf(e.getSQLState());
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

    private static void createTable(String statement) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(statement);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
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
    }

    private static void clearTable(String table) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM " + table + ";");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
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
    }

    public static void dropTable(String table) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS " + table);
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
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
    }

    private static String getTestsTableStatement() {
        return "CREATE TABLE " + TESTS + " ("             +
                    "ID INTEGER NOT NULL,\n"              +
                    "Semester INTEGER NOT NULL,\n"        +
                    "Time INTEGER NOT NULL,\n"            +
                    "Room INTEGER NOT NULL,\n"            +
                    "Day INTEGER NOT NULL,\n"             +
                    "CreditPoints INTEGER NOT NULL,\n"    +
                    "PRIMARY KEY(ID, SEMESTER),\n"        +
                    "CHECK(ID>0),\n"                      +
                    "CHECK(CreditPoints>0),\n"            +
                    "CHECK(Room>0)"                       +
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
                    "CHECK(CreditPoints>=0)"              +
                ")";
    }

    private static String getSupervisorsTableStatement() {
        return "CREATE TABLE " + SUPERVISORS + " ("       +
                    "ID INTEGER NOT NULL,\n"              +
                    "Name TEXT NOT NULL,\n"               +
                    "Salary INTEGER NOT NULL,\n"          +
                    "PRIMARY KEY(ID),\n"                  +
                    "CHECK(ID>0),\n"                      +
                    "CHECK(Salary>=0)"                    +
                ")";
    }

    private static String prepareAddStatement(String table, String[] attributes) {
        String statement = "INSERT INTO " + table + " (";
        for (int i=0; i<attributes.length; i++) {
            statement += attributes[i];
            if (i != attributes.length - 1) {
                statement += ", ";
            }
        }
        statement += ") ";
        statement += "VALUES (";
        for (int i=0; i<attributes.length; i++) {
            statement += "?";
            if (i != attributes.length - 1) {
                statement += ",";
            }
        }
        statement += ")";

        return statement;
    }

    private static PreparedStatement fillValuesInStatement(String statement, Object[] values, Object[] values_types) throws SQLException {
        // This function takes a statement that has (?, ?, ?) and "fills in" the values, so we get ("yossi", 1, "haifa")
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            // Making a statement object from a statement string
            pstmt = connection.prepareStatement(statement);
            for (int i=0; i<values.length; i++) {
                if (values_types[i] == TEXT) {
                    pstmt.setString(i+1, (String)values[i]);
                } else if (values_types[i] == INTEGER) {
                    pstmt.setInt(i+1, (int)values[i]);
                } 
                //TODO: complete for other types
            }
        } catch (SQLException e) {
            throw e;
        }
        return pstmt;
    }

    private static ReturnValue addToTable(String table, String[] attributes, Object[] values, Object[] values_types) {
        String statement = prepareAddStatement(table, attributes);
        try {
            PreparedStatement pstmt = fillValuesInStatement(statement, values, values_types);
            pstmt.execute();
        } catch (SQLException e) {
            if (compareSQLExceptions(e, PostgreSQLErrorCodes.NOT_NULL_VIOLATION)) {
                return BAD_PARAMS;
            } else if (compareSQLExceptions(e, PostgreSQLErrorCodes.CHECK_VIOLATION)) {
                return BAD_PARAMS;
            } else if (compareSQLExceptions(e, PostgreSQLErrorCodes.UNIQUE_VIOLATION)) {
                return ALREADY_EXISTS;
            } else {
                return ERROR;
            }
            //TODO: complete for other error types
        }
        return OK;
    }

    public static void createTables() {
        InitialState.createInitialState();
        createTable(getStudentsTableStatement());
        createTable(getSupervisorsTableStatement());
        createTable(getTestsTableStatement());
    } 

    public static void clearTables() {
        clearTable(TESTS);
        clearTable(STUDENTS);
        clearTable(SUPERVISORS);
    }

    public static void dropTables() {
        InitialState.dropInitialState();
        dropTable(TESTS);
        dropTable(STUDENTS);
        dropTable(SUPERVISORS);
    }

    public static ReturnValue addTest(Test test) {
        String[] attributes = {"ID", "Semester", "Time", "Room", "Day", "CreditPoints"};
        Object[] values = {test.getId(),test.getSemester(),test.getTime(),test.getRoom(),test.getDay(),test.getCreditPoints()};
        Object[] value_types = {INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER};
        return addToTable(TESTS, attributes, values, value_types);
    }

    public static Test getTestProfile(Integer testID, Integer semester) {
        return new Test();
    }

    public static ReturnValue deleteTest(Integer testID, Integer semester) {
		return OK;
    }

    public static ReturnValue addStudent(Student student) {
        return OK;
    }

    public static Student getStudentProfile(Integer studentID) {
        return new Student();
    }

    public static ReturnValue deleteStudent(Integer studentID) {
        return OK;
    }

    public static ReturnValue addSupervisor(Supervisor supervisor) {
        return OK;
    }

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        return new Supervisor();
    }

    public static ReturnValue deleteSupervisor(Integer supervisorID) {
        return OK;
    }

    public static ReturnValue studentAttendTest(Integer studentID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue studentWaiveTest(Integer studentID, Integer testID, Integer semester) {
        return OK;
    }

    public static ReturnValue supervisorOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
       return OK;
    }

    public static ReturnValue supervisorStopsOverseeTest(Integer supervisorID, Integer testID, Integer semester) {
       return OK;
    }

    public static Float averageTestCost() {
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

