
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.milaifontanals.club.Categoria;
import org.milaifontanals.club.DataException;
import org.milaifontanals.club.GestorBDClub;
import org.milaifontanals.club.GestorClubJDBC;
import org.milaifontanals.club.Jugador;
import org.milaifontanals.club.Sexe;
import org.milaifontanals.club.Temporada;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author beni
 */
public class Test {
    
    public static void main(String[] args) {
        GestorClubJDBC cp;
        try{
            System.out.println("Intent de creacio de la CP");
            cp = new GestorClubJDBC();
            System.out.println("s'ha fet la conexio a la BD! ");
            
        }catch (Exception ex){
            ex.printStackTrace();
            System.out.println("Problema en crear capa de persistència:");
            infoError(ex);
            System.out.println("Avortem programa");
            return;
        }
        
        
        
        /*
        Creacio de jugador
        */
        /*
        try {
            Calendar cal = Calendar.getInstance();
            cal.set(2017, Calendar.FEBRUARY, 2); 
            Date fechaNacimiento = cal.getTime();

            Jugador jugador = new Jugador("PRueba", "Carol Patata", Sexe.H, fechaNacimiento,
            "12345678A", "ES8731906713949384352621",
            "Carrer Carol 10", "./fotos/29430978V.png",
            2025);

            cp.afegirJugador(jugador);
            cp.confirmarCanvis();
        } catch (Exception ex) {
            System.out.println("Error al crear jugador : " + ex.getMessage());
            infoError(ex);
        }
        */
            
            
        /*try { 
            cp.esborrarJugador(46);
            cp.confirmarCanvis();
            System.out.println("Jugador esborrat");
        } catch (GestorBDClub ex) {
            System.out.println("Error al eliminar jugador : " + ex.getMessage());
            infoError(ex);
        }*/
        
        List<Temporada> temps = new ArrayList<>();
        try {
           temps = cp.obtenirLlistaTemporada();
            System.out.println(temps);
        } catch (GestorBDClub ex) {
            System.out.println("Error al obtenir totes les temporades: " + ex.getMessage());
            infoError(ex);
        }
        
        List<Categoria> cats = new ArrayList<>();
        try {
            cats = cp.obtenirLlistaCategoria();
            System.out.println(cats);
        } catch (GestorBDClub ex) {
            System.out.println("Error al obtenir totes les categories: " + ex.getMessage());
            infoError(ex);
        }
        
        
        List<Jugador> jgs = new ArrayList<>();
        try {
            jgs = cp.obtenirLlistaJugador();
            System.out.println(jgs);
        } catch (GestorBDClub ex) {
            System.out.println("Error al obtenir tots els jugadors: " + ex.getMessage());
            infoError(ex);
        }
        /*
        Tancament de CP
        */
        try{
            System.out.println("Tancament de la capa");
            cp.tancarCapa();
            System.out.println("Capa tancada");
        }catch(Exception ex){
            System.out.println("\tError: " + ex.getMessage());
            infoError(ex);
        }
    }
    
    private static void infoError(Throwable aux){
        do{
            if(aux.getMessage() != null){
                System.out.println("\t"+aux.getMessage());
            }
            aux = aux.getCause();
        }while(aux!=null);
    }
}
