import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class DBTest {

    public static boolean checkStub() {
        return true;
    }

    public static boolean isTypePresent(String typeName) {
        try {
            Class<?> clazz = Class.forName(typeName);
            return clazz.getName().equals(typeName);
            // return Modifier.isPublic(clazz.getModifiers());
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static boolean isTypeClass(String typeName) {
        try {
            Class<?> clazz = Class.forName(typeName);
            return !Modifier.isAbstract(clazz.getModifiers()) && !Modifier.isInterface(clazz.getModifiers());
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static boolean isMethodPresent(String typeName, String methodName, Class<?>[] parameterTypes,
                                          Class<?> parameterReturn) {
        // Class<?>[] parameterTypes = new Class<?>[] { List.class };
        // Class<?> parameterReturn = List.class;
        try {
            Class<?> clazz = Class.forName(typeName);
            Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
            //
            return Modifier.isPublic(method.getModifiers())
                    && parameterReturn.getName().equals(method.getReturnType().getName());
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public static String getTemporaryDBName() {
        //String timeTemplate = "yyyy-MM-dd_HH-mm-ss";
        String timeTemplate = "ddHHmmss";
        String currentTime = new SimpleDateFormat(timeTemplate).format(new Date());
        return "MyDB" + currentTime;
    }

    public static boolean checkCreateConnection() {
        //int origin = 0;
        //boolean expected = true;
        //
        //Connection actual = null;
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            Connection actual = myUtils.createConnection();
            result = actual != null;
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkCloseConnection() {
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            Connection actual = myUtils.createConnection();
            result = actual != null;
            myUtils.closeConnection();
            result = result && actual.isClosed();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkCreateStatement() {
        //int origin = 0;
        //boolean expected = true;
        //
        //Statement actual = null;
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            Statement actual = myUtils.createStatement();
            result = actual != null;
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkCloseStatement() {
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            Statement actual = myUtils.createStatement();
            result = actual != null;
            //
            myUtils.closeStatement();
            actual.close();
            result = result && actual.isClosed();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkCreateSchema() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkDropSchema() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.dropSchema();
            // Repeate
            myUtils.createSchema(schemaName);
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkUseSchema() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkCreateTableRoles() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkCreateTableDirections() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkCreateTableProjects() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkCreateTableEmployee() {
        String schemaName = getTemporaryDBName();
        //boolean expected = true;
        //
        //boolean actual = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkDropTable() {
        String schemaName = getTemporaryDBName();
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.dropTable("Roles");
            //
            myUtils.createTableRoles();
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean checkInsertTableRoles() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            //
            myUtils.insertTableRoles("Devel oper");
            result = myUtils.getRoleId("Developer") == 1;
            //
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkInsertTableDirections() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            //
            myUtils.insertTableDirections("Java");
            result = myUtils.getDirectionId("Java") == 1;
            //
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkInsertTableProjects() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            //
            myUtils.insertTableDirections("Golang");
            myUtils.insertTableProjects("Fujitsu", "Golang");
            result = myUtils.getProjectId("Fujitsu") == 1;
            //
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkInsertTableEmployee() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableDirections("Ruby");
            myUtils.insertTableProjects("IBM", "Ruby");
            myUtils.insertTableEmployee("William", "Developer", "IBM");
            result = myUtils.getEmployeeId("William") == 1;
            //
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkGetRoleId() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableRoles("QC");
            result = myUtils.getRoleId("QC") == 2;
            //
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkGetDirectionId() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            //
            myUtils.insertTableDirections("WebUI");
            myUtils.insertTableDirections(".Net");
            result = myUtils.getDirectionId(".Net") == 2;
            //
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkGetProjectId() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            //
            myUtils.insertTableDirections("Ruby");
            myUtils.insertTableProjects("IBM", "Ruby");
            myUtils.insertTableProjects("SAP", "Ruby");
            result = myUtils.getProjectId("SAP") == 2;
            //
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkGetEmployeeId() {
        String schemaName = getTemporaryDBName();
        boolean result = false;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableDirections("Java");
            myUtils.insertTableProjects("Fujitsu", "Java");
            myUtils.insertTableEmployee("Lucas", "Developer", "Fujitsu");
            myUtils.insertTableEmployee("Oliver", "Developer", "Fujitsu");
            result = myUtils.getEmployeeId("Oliver") == 2;
            //
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        return result;
    }

    public static boolean checkGetAllRoles() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList("DB", "DevOps", "Developer", "QC");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableRoles("DevOps");
            myUtils.insertTableRoles("QC");
            myUtils.insertTableRoles("DB");
            actual = myUtils.getAllRoles();
            //		.stream()
            //		.sorted()
            //		.collect(Collectors.toList());
            //
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
        //System.out.println(actual);
        return (actual != null) && expected.equals(actual);
    }

    public static boolean checkGetAllDirestion() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList(".Net", "Java", "Python", "Ruby", "WebUI");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            //
            myUtils.insertTableDirections("Java");
            myUtils.insertTableDirections(".Net");
            myUtils.insertTableDirections("Python");
            myUtils.insertTableDirections("Ruby");
            myUtils.insertTableDirections("WebUI");
            actual = myUtils.getAllDirestion();
            //
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
        return (actual != null) && expected.equals(actual);
    }

    public static boolean checkGetAllProjects() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList("Accentre", "Dell", "Fujitsu", "HP", "IBM");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            //
            myUtils.insertTableDirections("Java");
            myUtils.insertTableProjects("IBM", "Java");
            myUtils.insertTableProjects("Accentre", "Java");
            myUtils.insertTableProjects("Fujitsu", "Java");
            myUtils.insertTableProjects("Dell", "Java");
            myUtils.insertTableProjects("HP", "Java");
            actual = myUtils.getAllProjects();
            //
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
//        System.out.println(actual);
        return (actual != null) && expected.equals(actual);
    }

    public static boolean checkGetAllEmployee() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList("Emma", "James", "Oliver", "Olivia");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableDirections("Java");
            myUtils.insertTableProjects("MoonLight", "Java");
            myUtils.insertTableEmployee("James", "Developer", "MoonLight");
            myUtils.insertTableEmployee("Olivia", "Developer", "MoonLight");
            myUtils.insertTableEmployee("Emma", "Developer", "MoonLight");
            myUtils.insertTableEmployee("Oliver", "Developer", "MoonLight");
            actual = myUtils.getAllEmployee();
            //
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
//        System.out.println(actual);
        return (actual != null) && expected.equals(actual);
    }

    public static boolean checkGetAllDevelopers() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList("Isabella", "James", "Sophia", "William");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableRoles("DevOps");
            myUtils.insertTableRoles("QC");
            //
            myUtils.insertTableDirections("Java");
            myUtils.insertTableDirections("Python");
            //
            myUtils.insertTableProjects("Dell", "Java");
            myUtils.insertTableProjects("IBM", "Java");
            myUtils.insertTableProjects("HP", "Python");
            //
            myUtils.insertTableEmployee("James", "Developer", "Dell");
            myUtils.insertTableEmployee("William", "Developer", "IBM");
            myUtils.insertTableEmployee("Sophia", "Developer", "HP");
            myUtils.insertTableEmployee("Oliver", "DevOps", "Dell");
            myUtils.insertTableEmployee("Lucas", "DevOps", "HP");
            myUtils.insertTableEmployee("Isabella", "Developer", "Dell");
            myUtils.insertTableEmployee("Henry", "QC", "Dell");
            myUtils.insertTableEmployee("Emma", "QC", "IBM");
            myUtils.insertTableEmployee("Olivia", "QC", "HP");
            //
            actual = myUtils.getAllDevelopers();
            //
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
//        System.out.println(actual);
        return (actual != null) && expected.equals(actual);
    }

    public static boolean checkGetAllJavaProjects() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList("Accentre", "IBM");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableRoles("DevOps");
            myUtils.insertTableRoles("QC");
            //
            myUtils.insertTableDirections("Java");
            myUtils.insertTableDirections("Python");
            //
            myUtils.insertTableProjects("Accentre", "Java");
            myUtils.insertTableProjects("IBM", "Java");
            myUtils.insertTableProjects("Dell", "Python");
            //
            myUtils.insertTableEmployee("Oliver", "Developer", "Accentre");
            myUtils.insertTableEmployee("James", "Developer", "IBM");
            myUtils.insertTableEmployee("William", "Developer", "Dell");
            myUtils.insertTableEmployee("Lucas", "DevOps", "Accentre");
            myUtils.insertTableEmployee("Henry", "DevOps", "Dell");
            myUtils.insertTableEmployee("Olivia", "Developer", "Accentre");
            myUtils.insertTableEmployee("Emma", "QC", "Accentre");
            myUtils.insertTableEmployee("Sophia", "QC", "IBM");
            myUtils.insertTableEmployee("Isabella", "QC", "Dell");
            //
            actual = myUtils.getAllJavaProjects();
            //
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
//        System.out.println(actual);
        return (actual != null) && expected.equals(actual);
    }

    public static boolean checkGetAllJavaDevelopers() {
        String schemaName = getTemporaryDBName();
        List<String> expected = Arrays.asList("James", "Oliver", "Olivia");
        List<String> actual = null;
        try {
            MyUtils myUtils = new MyUtils();
            myUtils.createConnection();
            myUtils.createStatement();
            myUtils.createSchema(schemaName);
            myUtils.useSchema();
            //
            myUtils.createTableRoles();
            myUtils.createTableDirections();
            myUtils.createTableProjects();
            myUtils.createTableEmployee();
            //
            myUtils.insertTableRoles("Developer");
            myUtils.insertTableRoles("DevOps");
            myUtils.insertTableRoles("QC");
            //
            myUtils.insertTableDirections("Java");
            myUtils.insertTableDirections("Python");
            //
            myUtils.insertTableProjects("Accentre", "Java");
            myUtils.insertTableProjects("IBM", "Java");
            myUtils.insertTableProjects("Dell", "Python");
            //
            myUtils.insertTableEmployee("Oliver", "Developer", "Accentre");
            myUtils.insertTableEmployee("James", "Developer", "IBM");
            myUtils.insertTableEmployee("William", "Developer", "Dell");
            myUtils.insertTableEmployee("Lucas", "DevOps", "Accentre");
            myUtils.insertTableEmployee("Henry", "DevOps", "Dell");
            myUtils.insertTableEmployee("Olivia", "Developer", "Accentre");
            myUtils.insertTableEmployee("Emma", "QC", "Accentre");
            myUtils.insertTableEmployee("Sophia", "QC", "IBM");
            myUtils.insertTableEmployee("Isabella", "QC", "Dell");
            //
            actual = myUtils.getAllJavaDevelopers();
            //
            myUtils.dropTable("Employee");
            myUtils.dropTable("Projects");
            myUtils.dropTable("Directions");
            myUtils.dropTable("Roles");
            //
            myUtils.dropSchema();
            //
            myUtils.closeStatement();
            myUtils.closeConnection();
        } catch (Exception e) {
            return false;
        }
        Collections.sort(actual);
//        System.out.println(actual);
        return (actual != null) && expected.equals(actual);
    }

}
