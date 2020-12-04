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
        if (test.getId() <= 0 || test.getCreditPoints() <= 0 || test.getRoom() <= 0) {
            return BAD_PARAMS;
        }
        if (false) {
            // check if already exists
        }
        try {

        } catch (Exception e) {
            return ERROR;
        }
       return OK;
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

