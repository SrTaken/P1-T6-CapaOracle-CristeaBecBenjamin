
import org.milaifontanals.club.GestorBDClub;
import org.milaifontanals.club.GestorClubJDBC;

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
        
        try{
            System.out.println("Tancament de la capa");
            cp.tancarCapa();
            System.out.println("Capa tancada");
        }catch(Exception ex){
            System.out.println("\tError: " + ex.getMessage());
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
