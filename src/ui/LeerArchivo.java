package ui;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class LeerArchivo {
    
    private String archivo;
    private String separador;
    private List<List<String>> datos = new ArrayList<>();
    
    public LeerArchivo(String archivo, String separador) {
        this.archivo = archivo;
        this.separador = separador;
        leer();
    }
    
    public void leer() {
        
        try (Scanner scanner = new Scanner(new File(archivo))) {
            while (scanner.hasNextLine()) {
                String linea = scanner.nextLine();
                List<String> fila = Arrays.asList(linea.trim().split(separador));
                datos.add(fila);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }


    public void enviarInformacion(String tabla, List<List<String>> datos, int numColumnas) {
        try (Connection conn = ConexionOracle.obtenerConexion();
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + tabla + " VALUES (" + String.join(",", Collections.nCopies(numColumnas, "?")) + ")")) {
            conn.setAutoCommit(false);
            for (List<String> fila : datos) {
                String[] filaArray = fila.toArray(new String[numColumnas]);
                for (int i = 0; i < numColumnas; i++) {
                    stmt.setString(i + 1, filaArray[i]);
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
            System.out.println("La información ha sido enviada a la base de datos.");
        } catch (SQLException ex) {
            System.out.println("Error al enviar la información a la base de datos: " + ex.getMessage());
        }
    }
    
    public List<List<String>> getDatos() {
        return datos;
    }
    

    
    
}
