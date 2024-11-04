import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public class MyUtilsTest {
	public static void main(String[] args) throws Exception {

		System.out.println(DBTest.checkStub());
		System.out.println("TypePresent - " + DBTest.isTypePresent("MyUtils"));
		System.out.println("TypeClass - " + DBTest.isTypeClass("MyUtils"));
		//
		System.out.println("createConnection - " + DBTest.isMethodPresent("MyUtils", "createConnection", new Class<?>[] { }, Connection.class));
		System.out.println("closeConnection - " + DBTest.isMethodPresent("MyUtils", "closeConnection", new Class<?>[] { }, void.class));
		System.out.println("createStatement - " + DBTest.isMethodPresent("MyUtils", "createStatement", new Class<?>[] { }, Statement.class));
		System.out.println("closeStatement - " + DBTest.isMethodPresent("MyUtils", "closeStatement", new Class<?>[] { }, void.class));
		System.out.println("createSchema - " + DBTest.isMethodPresent("MyUtils", "createSchema", new Class<?>[] { String.class }, void.class));
		System.out.println("dropSchema - " + DBTest.isMethodPresent("MyUtils", "dropSchema", new Class<?>[] { }, void.class));
		System.out.println("useSchema - " + DBTest.isMethodPresent("MyUtils", "useSchema", new Class<?>[] { }, void.class));
		System.out.println("createTableRoles - " + DBTest.isMethodPresent("MyUtils", "createTableRoles", new Class<?>[] { }, void.class));
		System.out.println("createTableDirections - " + DBTest.isMethodPresent("MyUtils", "createTableDirections", new Class<?>[] { }, void.class));
		System.out.println("createTableProjects - " + DBTest.isMethodPresent("MyUtils", "createTableProjects", new Class<?>[] { }, void.class));
		System.out.println("createTableEmployee - " + DBTest.isMethodPresent("MyUtils", "createTableEmployee", new Class<?>[] { }, void.class));
		System.out.println("dropTable - " + DBTest.isMethodPresent("MyUtils", "dropTable", new Class<?>[] { String.class }, void.class));
		System.out.println("insertTableRoles - " + DBTest.isMethodPresent("MyUtils", "insertTableRoles", new Class<?>[] { String.class }, void.class));
		System.out.println("insertTableDirections - " + DBTest.isMethodPresent("MyUtils", "insertTableDirections", new Class<?>[] { String.class }, void.class));
		System.out.println("insertTableProjects - " + DBTest.isMethodPresent("MyUtils", "insertTableProjects", new Class<?>[] { String.class, String.class }, void.class));
		System.out.println("insertTableEmployee - " + DBTest.isMethodPresent("MyUtils", "insertTableEmployee", new Class<?>[] { String.class, String.class, String.class }, void.class));
		System.out.println("getRoleId - " + DBTest.isMethodPresent("MyUtils", "getRoleId", new Class<?>[] { String.class }, int.class));
		System.out.println("getDirectionId - " + DBTest.isMethodPresent("MyUtils", "getDirectionId", new Class<?>[] { String.class }, int.class));
		System.out.println("getProjectId - " + DBTest.isMethodPresent("MyUtils", "getProjectId", new Class<?>[] { String.class }, int.class));
		System.out.println("getEmployeeId - " + DBTest.isMethodPresent("MyUtils", "getEmployeeId", new Class<?>[] { String.class }, int.class));
		System.out.println("getAllRoles - " + DBTest.isMethodPresent("MyUtils", "getAllRoles", new Class<?>[] { }, List.class));
		System.out.println("getAllDirestion - " + DBTest.isMethodPresent("MyUtils", "getAllDirestion", new Class<?>[] { }, List.class));
		System.out.println("getAllProjects - " + DBTest.isMethodPresent("MyUtils", "getAllProjects", new Class<?>[] { }, List.class));
		System.out.println("getAllEmployee - " + DBTest.isMethodPresent("MyUtils", "getAllEmployee", new Class<?>[] { }, List.class));
		System.out.println("getAllDevelopers - " + DBTest.isMethodPresent("MyUtils", "getAllDevelopers", new Class<?>[] { }, List.class));
		System.out.println("getAllJavaProjects - " + DBTest.isMethodPresent("MyUtils", "getAllJavaProjects", new Class<?>[] { }, List.class));
		System.out.println("getAllJavaDevelopers - " + DBTest.isMethodPresent("MyUtils", "getAllJavaDevelopers", new Class<?>[] { }, List.class));
		//
		//
		System.out.println("checkCreateConnection - " + DBTest.checkCreateConnection());
		System.out.println("checkCloseConnection - " + DBTest.checkCloseConnection());
		System.out.println("checkCreateStatement - " + DBTest.checkCreateStatement());
		System.out.println("checkCloseStatement - " + DBTest.checkCloseStatement());
		System.out.println("checkCreateSchema - " + DBTest.checkCreateSchema());
		System.out.println("checkDropSchema - " + DBTest.checkDropSchema());
		System.out.println("checkUseSchema - " + DBTest.checkUseSchema());
		//
		System.out.println("checkCreateTableRoles - " + DBTest.checkCreateTableRoles());
		System.out.println("checkCreateTableDirections - " + DBTest.checkCreateTableDirections());
		System.out.println("checkCreateTableProjects - " + DBTest.checkCreateTableProjects());
		System.out.println("checkCreateTableEmployee - " + DBTest.checkCreateTableEmployee());
		//
		System.out.println("checkDropTable - " + DBTest.checkDropTable());
		System.out.println("checkInsertTableRoles - " + DBTest.checkInsertTableRoles());
		System.out.println("checkInsertTableDirections - " + DBTest.checkInsertTableDirections());
		System.out.println("checkInsertTableProjects - " + DBTest.checkInsertTableProjects());
		System.out.println("checkInsertTableEmployee - " + DBTest.checkInsertTableEmployee());
		//
		System.out.println("checkGetRoleId - " + DBTest.checkGetRoleId());
		System.out.println("checkGetDirectionId - " + DBTest.checkGetDirectionId());
		System.out.println("checkGetProjectId - " + DBTest.checkGetProjectId());
		System.out.println("checkGetEmployeeId - " + DBTest.checkGetEmployeeId());
		//
		System.out.println("checkGetAllRoles - " + DBTest.checkGetAllRoles());
		System.out.println("checkGetAllDirestion - " + DBTest.checkGetAllDirestion());
		System.out.println("checkGetAllProjects - " + DBTest.checkGetAllProjects());
		System.out.println("checkGetAllEmployee - " + DBTest.checkGetAllEmployee());
		//
		System.out.println("checkGetAllDevelopers - " + DBTest.checkGetAllDevelopers());
		System.out.println("checkGetAllJavaProjects - " + DBTest.checkGetAllJavaProjects());
		System.out.println("checkGetAllJavaDevelopers - " + DBTest.checkGetAllJavaDevelopers());


	}

}
