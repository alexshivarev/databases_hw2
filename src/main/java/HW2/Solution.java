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

    private static void createTable(String table) {
        String statement = "";
        if (table == TESTS) {
            statement = getTestsTableStatement();
        } else if (table == STUDENTS) {
            statement = getStudentsTableStatement();
        } else if (table == SUPERVISORS) {
            statement = getSupervisorsTableStatement();
        } 
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
        createTable(STUDENTS);
        createTable(SUPERVISORS);
        createTable(TESTS);
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
        Object[] values = {test.getId(), test.getSemester(), test.getTime(), test.getRoom(), test.getDay(), test.getCreditPoints()};
        Object[] value_types = {INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER};
        ReturnValue retval = addToTable(TESTS, attributes, values, value_types);
        return retval;
    }

    private static String prepareSelectStatement(String table, Object[] attributes_to_select, Object[] attributes_for_where, Object[] values){
        String attributes_string = "";
        for (int i = 0; i < attributes_to_select.length; i++) {
            attributes_string += attributes_to_select[i];
            if (i != attributes_to_select.length - 1) {
                attributes_string += ", ";
            }
        }
        String statement = "SELECT " + attributes_string + " FROM " + table;
        for (int i = 0; i < attributes_for_where.length; i++) {
                if (i == 0) {
                    statement += "\nWHERE ";
                }
                statement += attributes_for_where[i];
                statement += " = ";
                statement += values[i]; 
                if (i != attributes_for_where.length - 1) {
                    statement += " AND ";
                }
            }
        return statement;
    }

    private static ResultSet selectFromDB(String table, Object[] attributes_to_select, Object[] attributes_for_where, Object[] values) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String statement = prepareSelectStatement(table, attributes_to_select , attributes_for_where, values);
            pstmt = connection.prepareStatement(statement);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        /* TODO: understand what this is needed for. I copied it from the example.
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        */
        return rs; 
    }

    public static Test getTestProfile(Integer testID, Integer semester) {
        Test test = new Test();
        ResultSet rs = selectFromDB(TESTS, new Object[] {"*"}, new Object[] {"ID", "Semester"}, new Object[] {testID, semester});
        try {
            if (rs.next() == false) {
                return Test.badTest();
            }
            test.setId(rs.getInt(1));
            test.setSemester(rs.getInt(2));
            test.setTime(rs.getInt(3));
            test.setRoom(rs.getInt(4));
            test.setDay(rs.getInt(5));
            test.setCreditPoints(rs.getInt(6));
            rs.close();
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
            pstmt = connection.prepareStatement(statement);
            affectedRows = pstmt.executeUpdate();
            System.out.println("deleted " + affectedRows + " rows");
        } catch (SQLException e) {
            return -1;
        }
        /* TODO: understand what this is for
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        */
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

    public static Student getStudentProfile(Integer studentID) {
        Student student = new Student();
        ResultSet rs = selectFromDB(STUDENTS, new Object[] {"*"}, new Object[] {"ID"}, new Object[] {studentID});
        try {
            if (rs.next() == false) {
                return Student.badStudent();
            }
            student.setId(rs.getInt(1));
            student.setName(rs.getString(2));
            student.setFaculty(rs.getString(3));
            student.setCreditPoints(rs.getInt(4));
            rs.close();
        } catch (SQLException e) {
            return student.badStudent();
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

    public static Supervisor getSupervisorProfile(Integer supervisorID) {
        Supervisor supervisor = new Supervisor();
        ResultSet rs = selectFromDB(SUPERVISORS, new Object[] {"*"}, new Object[] {"ID"}, new Object[] {supervisorID});
        try {
            if (rs.next() == false) {
                return Supervisor.badSupervisor();
            }
            supervisor.setId(rs.getInt(1));
            supervisor.setName(rs.getString(2));
            supervisor.setSalary(rs.getInt(3));
            rs.close();
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

