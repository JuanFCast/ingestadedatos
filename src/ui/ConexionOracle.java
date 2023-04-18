package ui;

import java.sql.*;


public class ConexionOracle {

    public static void main(String[] args) {

        Connection conexion = null;
    
        try {
            // Cargar el driver JDBC
            Class.forName("oracle.jdbc.driver.OracleDriver");
    
            // Establecer la conexión con la base de datos
            conexion = ConexionOracle.obtenerConexion();

            // Realizar operaciones en la base de datos

            Statement stmt = conexion.createStatement();
            String Data = "CREATE TABLE DATA (USERID NUMBER(38,0), ITEMID NUMBER(38,0), RATING NUMBER(38,0), TIMESTAMP NUMBER(38,0))";
            String Genre = "CREATE TABLE GENRE (GENRES VARCHAR2(50), ID NUMBER(38,0))";
            String Info = "CREATE TABLE INFO (AMOUNT NUMBER(38,0), DATASET VARCHAR2(50))";
            String Item = "CREATE TABLE ITEM (MOVIEID NUMBER(38,0), MOVIETITLE VARCHAR2(150), RELEASEDATE VARCHAR2(150), VIDEODATE VARCHAR2(150), IMDBURL VARCHAR2(150), UNKNOWM NUMBER(38,0), ACTION NUMBER(38,0), ADVENTURE NUMBER(38,0), ANIMATION NUMBER(38,0), CHILDREN NUMBER(38,0), COMEDY NUMBER(38,0), CRIME NUMBER(38,0), DOCUMENTARY NUMBER(38,0), DRAMA NUMBER(38,0), FANTASY NUMBER(38,0), FILM_NOIR NUMBER(38,0), HORROR NUMBER(38,0), MUSICAL NUMBER(38,0), MYSTERY NUMBER(38,0), ROMANCE NUMBER(38,0), SCI_FI NUMBER(38,0), THRILLER NUMBER(38,0), WAR NUMBER(38,0), WESTERN NUMBER(38,0))";
            String Occupation = "CREATE TABLE OCCUPATION (OCCUPATIONS VARCHAR2(50))";
            String Users = "CREATE TABLE USERS (USER_ID NUMBER(38,0), AGE NUMBER(38,0), GENDER VARCHAR2(50), OCCUPATION VARCHAR2(50), ZIP_CODE VARCHAR2(50))";

            stmt.execute(Data);
            stmt.execute(Genre);
            stmt.execute(Info);
            stmt.execute(Item);
            stmt.execute(Occupation);
            stmt.execute(Users);

            System.out.println("Tablas creadas exitosamente.");

            LeerArchivo lectorData = new LeerArchivo("ml-100k/u.data","\t");
            LeerArchivo lectorGenre = new LeerArchivo("ml-100k/u.genre","\\|");
            LeerArchivo lectorInfo = new LeerArchivo("ml-100k/u.info","\\s+");
            LeerArchivo lectorItem = new LeerArchivo("ml-100k/u.item","\\|");
            LeerArchivo lectorOccupation = new LeerArchivo("ml-100k/u.occupation","\n");
            LeerArchivo lectorUser = new LeerArchivo("ml-100k/u.user","\\|");

            

            lectorData.enviarInformacion("DATA", lectorData.getDatos(), 4);
            lectorGenre.enviarInformacion("GENRE", lectorGenre.getDatos(), 2);
            lectorInfo.enviarInformacion("INFO", lectorInfo.getDatos(), 2);
            lectorItem.enviarInformacion("ITEM", lectorItem.getDatos(), 24);
            lectorOccupation.enviarInformacion("OCCUPATION", lectorOccupation.getDatos(), 1);
            lectorUser.enviarInformacion("USERS", lectorUser.getDatos(), 5);

            // Cerrar la conexión con la base de datos
            conexion.close();
    
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Error al realizar operaciones en la base de datos.");
            e.printStackTrace();
        } finally {
            // Cerrar la conexión con la base de datos (en caso de error)
            if (conexion != null) {
                try {
                    conexion.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Connection obtenerConexion() {
        Connection conexion = null;
        try {
            // Obtener la conexión a la base de datos
            String url = "jdbc:oracle:thin:@200.3.193.24:1522/ESTUD";
            String usuario = "P09551_1_2";
            String password = "pipocastillo";
            conexion = DriverManager.getConnection(url, usuario, password);
            System.out.println("Conexión exitosa!");
        } catch (SQLException ex) {
            System.out.println("Error al obtener la conexión: " + ex.getMessage());
        }
        return conexion;
    }
    

}
