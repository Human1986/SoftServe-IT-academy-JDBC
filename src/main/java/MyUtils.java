import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MyUtils {
    private Connection connection;
    private Statement statement;
    private String schemaName;

    private static int getId(String name, ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            throw new SQLException("Name not found: " + name);
        }
    }

    public Connection createConnection() throws SQLException, ClassNotFoundException {
        DriverManager.registerDriver(new org.h2.Driver());
        connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");

        return connection;
    }

    public void closeConnection() throws SQLException {
        connection.close();
    }

    public Statement createStatement() throws SQLException {
        statement = connection.createStatement();
        return statement;
    }

    public void closeStatement() throws SQLException {
        statement.close();
    }

    public void createSchema(String schemaName) throws SQLException {
        this.schemaName = schemaName;
        String queryToCreateScheme = "CREATE SCHEMA " + schemaName;
        statement.execute(queryToCreateScheme);
    }

    public void dropSchema() throws SQLException {
        String queryToDropScheme = "DROP SCHEMA " + schemaName;
        statement.execute(queryToDropScheme);
    }

    public void useSchema() throws SQLException {
        statement.execute("SET SCHEMA " + schemaName);
    }

    public void createTableRoles() throws SQLException {
        String createTableRoles = " CREATE table IF NOT EXISTS Roles (" +
                                  " id serial PRIMARY KEY, " +
                                  " roleName varchar(32));";

        statement.execute(createTableRoles);
    }

    public void createTableDirections() throws SQLException {
        String createTableDirections =
                " CREATE table IF NOT EXISTS Directions (" +
                " id serial PRIMARY KEY, " +
                " directionName varchar(32) NOT NULL)";

        statement.execute(createTableDirections);
    }

    public void createTableProjects() throws SQLException {
        String createTableProjects =
                " CREATE table IF NOT EXISTS Projects (" +
                " id serial PRIMARY KEY, " +
                " projectName varchar (32) NOT NULL, " +
                " directionId int, " +
                " FOREIGN KEY(directionId) REFERENCES Directions (id))";

        statement.execute(createTableProjects);
    }

    public void createTableEmployee() throws SQLException {
        String createTableEmployee =
                " CREATE table IF NOT EXISTS Employee (" +
                " id serial PRIMARY KEY, " +
                " firstName varchar (32) NOT NULL, " +
                " roleId int, " +
                " projectId int, " +
                " FOREIGN KEY (roleId) REFERENCES Roles (id), " +
                " FOREIGN KEY (projectId) REFERENCES Projects (id))";

        statement.execute(createTableEmployee);
    }

    public void dropTable(String tableName) throws SQLException {
        String dropTable = "DROP TABLE " + tableName;
        statement.execute(dropTable);
    }

    public void insertTableRoles(String roleName) throws SQLException {
        String role = roleName.replace(" ", "");
        String insertRole = "INSERT INTO Roles (roleName) VALUES (?)";

        try (PreparedStatement roleStmt = connection.prepareStatement(insertRole)) {
            roleStmt.setString(1, role);
            roleStmt.executeUpdate();
        }
    }

    public void insertTableDirections(String directionName) throws SQLException {
        String insertDirection = "INSERT INTO Directions (directionName) VALUES (?)";

        try (PreparedStatement directionStmt = connection.prepareStatement(insertDirection)) {
            directionStmt.setString(1, directionName);
            directionStmt.executeUpdate();
        }
    }

    public void insertTableProjects(String projectName, String directionName) throws SQLException {
        int directId = getDirectionId(directionName);
        String insertProject = "INSERT INTO Projects (projectName, directionId) VALUES (?, ?)";
        String insertdirection = "INSERT INTO Directions (directionName) VALUES (?)";

        try (PreparedStatement projectStmt = connection.prepareStatement(insertProject);
             PreparedStatement directionStmt = connection.prepareStatement(insertdirection, Statement.RETURN_GENERATED_KEYS)) {

            int directionId;
            if (directId == 0) {
                directionStmt.setString(1, directionName);
                directionStmt.executeUpdate();
                ResultSet directionKeys = directionStmt.getGeneratedKeys();
                directionId = directionKeys.next() ? directionKeys.getInt(1) : 0;
            } else {
                directionId = directId;
            }


            projectStmt.setString(1, projectName);
            projectStmt.setInt(2, directionId);
            projectStmt.executeUpdate();
        }
    }

    public void insertTableEmployee(String firstName, String roleName, String projectName) throws SQLException {
        int rId = getRoleId(roleName);
        int pId = getProjectId(projectName);
        int roleId;
        int projectId;
        String role = "INSERT INTO Roles (roleName) VALUES (?)";
        String project = "INSERT INTO Projects (projectName) VALUES (?)";
        String employee = "INSERT INTO Employee (firstName, roleId, projectId) VALUES (?,?,?)";

        try (PreparedStatement roleStmt = connection.prepareStatement(role, Statement.RETURN_GENERATED_KEYS);
             PreparedStatement projectStmt = connection.prepareStatement(project, Statement.RETURN_GENERATED_KEYS);
             PreparedStatement emplStmt = connection.prepareStatement(employee)) {

            if (rId == 0) {
                roleStmt.setString(1, roleName);
                roleStmt.executeUpdate();
                ResultSet roleKeys = roleStmt.getGeneratedKeys();
                roleId = roleKeys.next() ? roleKeys.getInt(1) : 0;
            } else roleId = rId;


            if (pId == 0) {
                projectStmt.setString(1, projectName);
                projectStmt.executeUpdate();
                ResultSet projectKeys = projectStmt.getGeneratedKeys();
                projectId = projectKeys.next() ? projectKeys.getInt(1) : 0;
            } else projectId = pId;

            emplStmt.setString(1, firstName);
            emplStmt.setInt(2, roleId);
            emplStmt.setInt(3, projectId);

            emplStmt.executeUpdate();
        }
    }

    public int getRoleId(String roleName) throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT id FROM Roles WHERE roleName = '" + roleName + "'");
        return getId(roleName, resultSet);

    }

    public int getDirectionId(String directionName) throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT id FROM Directions WHERE directionName = '" + directionName + "'");
        return getId(directionName, resultSet);
    }

    public int getProjectId(String projectName) throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT id FROM Projects WHERE projectName = '" + projectName + "'");
        return getId(projectName, resultSet);
    }

    public int getEmployeeId(String firstName) throws SQLException {

        ResultSet resultSet = statement.executeQuery("SELECT id FROM Employee WHERE firstName = '" + firstName + "'");
        return getId(firstName, resultSet);
    }

    public List<String> getAllRoles() throws SQLException {
        List<String> roles = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery("SELECT roleName FROM Roles");

        while (resultSet.next()) {
            String roleName = resultSet.getString("roleName");
            roles.add(roleName);
        }

        return roles;
    }

    public List<String> getAllDirestion() throws SQLException {
        List<String> directions = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery("SELECT directionName FROM Directions");

        while (resultSet.next()) {
            String directionName = resultSet.getString("directionName");
            directions.add(directionName);
        }

        return directions;
    }

    public List<String> getAllProjects() throws SQLException {
        List<String> projects = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery("SELECT projectName FROM Projects");

        while (resultSet.next()) {
            String projectName = resultSet.getString("projectName");
            projects.add(projectName);
        }

        return projects;
    }

    public List<String> getAllEmployee() throws SQLException {
        List<String> employees = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery("SELECT firstName FROM Employee");

        while (resultSet.next()) {
            String employeeName = resultSet.getString("firstName");
            employees.add(employeeName);
        }

        return employees;
    }

    public List<String> getAllDevelopers() throws SQLException {
        List<String> developers = new ArrayList<>();
        String sqlQuery = " SELECT e.firstName FROM Employee e " +
                          " JOIN Roles r ON e.roleId = r.id " +
                          " WHERE r.roleName = 'Developer' " +
                          " ORDER BY e.firstName ASC";

        ResultSet resultSet = statement.executeQuery(sqlQuery);

        while (resultSet.next()) {
            String developerName = resultSet.getString("firstName");
            developers.add(developerName);
        }

        return developers;
    }

    public List<String> getAllJavaProjects() throws SQLException {
        List<String> javaProjects = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery(
                " SELECT p.projectName FROM Projects p " +
                " JOIN Directions d ON p.directionId = d.id " +
                " WHERE d.directionName = 'Java'" +
                " ORDER BY p.projectName ASC");

        while (resultSet.next()) {
            String projectName = resultSet.getString("projectName");
            javaProjects.add(projectName);
        }

        return javaProjects;
    }

    public List<String> getAllJavaDevelopers() throws SQLException {
        List<String> javaDevelopers = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery(
                " SELECT e.firstName FROM Employee e " +
                " JOIN Roles r ON e.roleId = r.id " +
                " JOIN Projects p ON e.projectId = p.id" +
                " JOIN Directions d ON p.directionId = d.id" +
                " WHERE r.roleName = 'Developer' AND d.directionName = 'Java'" +
                " ORDER BY e.firstName ASC");

        while (resultSet.next()) {
            String employeeName = resultSet.getString("firstName");
            javaDevelopers.add(employeeName);
        }

        return javaDevelopers;
    }

}
