/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.milaifontanals.club.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.sql.Date;
import java.util.ArrayList;
import java.sql.Statement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import org.milaifontanals.club.Categoria;
import org.milaifontanals.club.DataException;
import org.milaifontanals.club.Equip;
import org.milaifontanals.club.GestorBDClub;
import org.milaifontanals.club.IClubOracleBD;
import org.milaifontanals.club.Jugador;
import org.milaifontanals.club.Membre;
import org.milaifontanals.club.Sexe;
import org.milaifontanals.club.Temporada;
import org.milaifontanals.club.Tipus;
import org.milaifontanals.club.Usuari;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.HashMap;

/**
 *
 * @author beni
 */
public class GestorClubJDBC implements IClubOracleBD{

    private Connection conn;
    
    private PreparedStatement psInsertJugador;
    private PreparedStatement psDelJugador;
    private PreparedStatement psModificarJugador;
    private PreparedStatement psInsertTemporada;
    private PreparedStatement psDelTemporada;
    private PreparedStatement psInsertCategoria;
    private PreparedStatement psDelCategoria;
    private PreparedStatement psInsertEquip;
    private PreparedStatement psDelEquip;
    private PreparedStatement psModificarEquip;
    private PreparedStatement psInsertMembre;
    private PreparedStatement psDelMembre;
    private PreparedStatement psInsertUsuari;
    private PreparedStatement psDelUsuari;
    private PreparedStatement psModificarUsuari;
    private PreparedStatement psValidarLogin;
    private PreparedStatement psObtenirUsuari;
    private PreparedStatement psObtenirCategoria;
    private PreparedStatement psObtenirTemporada;
    private PreparedStatement psSelectMembresByEquip;
    private PreparedStatement psSelectEquip;
    private PreparedStatement psUpdateMembre;
    private PreparedStatement psSelectMembre;
    private PreparedStatement psSelectJugador;
    
    private static SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy");

    private Map<Integer, Jugador> jugadorCache = new HashMap<>();
    private Map<String, Membre> membreCache = new HashMap<>(); // key: jugadorId_equipId
    private Map<Integer, Equip> equipCache = new HashMap<>();
    private Map<Integer, Temporada> temporadaCache = new HashMap<>();
    private Map<Integer, Categoria> categoriaCache = new HashMap<>();

    public GestorClubJDBC() throws GestorBDClub{
        this("clubDB.properties");
    }
    
    public GestorClubJDBC(String nomFitxerProperties) throws GestorBDClub {
        if(nomFitxerProperties == null || nomFitxerProperties.equals(""))
            throw new GestorBDClub("Nom de fitxer no valid! ");
        
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(nomFitxerProperties));
        } catch (FileNotFoundException ex) {
           throw new GestorBDClub("No es troba el fitxer de propiertats" + nomFitxerProperties, ex);
        } catch (IOException ex) {
            throw new GestorBDClub("Error en intentar carregar el fitxer de propiertats" + nomFitxerProperties, ex);
        }
        
        String url = props.getProperty("url");
        String user = props.getProperty("user");
        String password = props.getProperty("password");
        
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new GestorBDClub("No ha sigut posible fer la conexio amb la BD", ex);
        }
        
    }
    
    
    @Override
    public void tancarCapa() throws GestorBDClub {
        if(conn == null)
            throw new GestorBDClub("No hi ha ninguna conexio amb la BD creada");
        
        try {
            conn.rollback();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al fer el rollback final. ", ex);
        }
        
        try {
            conn.close();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en tancar la connexió.\n", ex);
        }
    }
    
    @Override
    public void confirmarCanvis() throws GestorBDClub{
        try {
            conn.commit();
        } catch (SQLException ex) {
            clearCaches(); // Limpiar caches en caso de error
            throw new GestorBDClub("Error en confirmar canvis", ex);
        }
    }

    @Override
    public void afegirJugador(Jugador j) throws GestorBDClub {
        if (psInsertJugador == null) {
            try {
                psInsertJugador = conn.prepareStatement("INSERT INTO jugador (nom, cognoms, data_naix, sexe, adreça, foto, any_fi_revisió_mèdica, IBAN, idLegal, poblacio, cp) "
                                                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psInsertJugador", ex);
            }
        }
        try {
            psInsertJugador.setString(1, j.getNom());
            psInsertJugador.setString(2, j.getCognom());
            psInsertJugador.setDate(3, new java.sql.Date(j.getData_naix().getTime()));
            psInsertJugador.setString(4, j.getSexeString());
            psInsertJugador.setString(5, j.getAdresa());
            psInsertJugador.setString(6, j.getFoto());
            psInsertJugador.setInt(7, j.getAny_fi_revisio_medica());
            psInsertJugador.setString(8, j.getIban());
            psInsertJugador.setString(9, j.getIdLegal());
            psInsertJugador.setString(10, j.getPoblacio());
            psInsertJugador.setInt(11, j.getCp());
            
            psInsertJugador.executeUpdate();
            jugadorCache.put(j.getId(), j);
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en inserir jugador", ex);
        }
    }

    @Override
    public void esborrarJugador(int id) throws GestorBDClub {
        if (psDelJugador == null) {
            try {
                psDelJugador = conn.prepareStatement("DELETE FROM jugador WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psDelListProduct", ex);
            }
        }
        try {
            psDelJugador.setInt(1, id);
            if(psDelJugador.executeUpdate() < 1)
                throw new GestorBDClub("No s'ha esborrat cap jugador ");
                
            jugadorCache.remove(id);
            membreCache.entrySet().removeIf(entry -> 
                entry.getKey().startsWith(id + "_"));
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en esborrar jugador", ex);
        }
    }

    
    @Override
    public void desferCanvis() throws GestorBDClub {
        try {
            conn.rollback();
            clearCaches(); // Limpiar caches después de rollback
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en desfer canvis", ex);
        }
    }
    
    @Override
    public void modificarJugador(Jugador j) throws GestorBDClub {
        if (psModificarJugador == null) {
            try {
                psModificarJugador = conn.prepareStatement("UPDATE jugador SET nom = ?, cognoms = ?, data_naix = ?, sexe = ?, adreça = ?, foto = ?, any_fi_revisió_mèdica = ?, IBAN = ?, idLegal = ?, poblacio = ?, cp = ? WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psModificarJugador", ex);
            }
        }
        try {
            psModificarJugador.setString(1, j.getNom());
            psModificarJugador.setString(2, j.getCognom());
            psModificarJugador.setDate(3, new java.sql.Date(j.getData_naix().getTime()));
            psModificarJugador.setString(4, j.getSexeString());
            psModificarJugador.setString(5, j.getAdresa());
            psModificarJugador.setString(6, j.getFoto());
            psModificarJugador.setInt(7, j.getAny_fi_revisio_medica());
            psModificarJugador.setString(8, j.getIban());
            psModificarJugador.setString(9, j.getIdLegal());
            psModificarJugador.setString(10, j.getPoblacio());
            psModificarJugador.setInt(11, j.getCp());
            psModificarJugador.setInt(12, j.getId());
            
            if (psModificarJugador.executeUpdate() == 0) {
                throw new GestorBDClub("No se encontró el jugador para modificar");
            }
            jugadorCache.put(j.getId(), j);
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en modificar jugador", ex);
        }
    }

    @Override
    public void afegirTemporada(Temporada t) throws GestorBDClub {
        if (psInsertTemporada == null) {
            try {
                psInsertTemporada = conn.prepareStatement("INSERT INTO temporada (year) VALUES (?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psInsertTemporada", ex);
            }
        }
        try {
            psInsertTemporada.setInt(1, t.getYear());
            psInsertTemporada.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al insertar la temporada", ex);
        }
    }


    @Override
    public void esborrarTemporada(int year) throws GestorBDClub {
        if (psDelTemporada == null) {
            try {
                psDelTemporada = conn.prepareStatement("DELETE FROM temporada WHERE year = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psDelTemporada", ex);
            }
        }
        try {
            psDelTemporada.setInt(1, year);
            if (psDelTemporada.executeUpdate() == 0) {
                throw new GestorBDClub("No se ha eliminado ninguna temporada con el año especificado");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al intentar eliminar la temporada", ex);
        }
    }

    @Override
    public void afegirCategoria(Categoria c) throws GestorBDClub {
        if (psInsertCategoria == null) {
            try {
                psInsertCategoria = conn.prepareStatement("INSERT INTO categoria (nom, edat_min, edat_max) VALUES (?, ?, ?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psInsertCategoria", ex);
            }
        }
        try {
            psInsertCategoria.setString(1, c.getCategoria());
            psInsertCategoria.setInt(2, c.getEdat_minima());
            psInsertCategoria.setInt(3, c.getEdat_maxima());
            psInsertCategoria.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al insertar la categoría", ex);
        }
    }

    @Override
    public void esborrarCategoria(Categoria c) throws GestorBDClub {
        if (psDelCategoria == null) {
            try {
                psDelCategoria = conn.prepareStatement("DELETE FROM categoria WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psDelCategoria", ex);
            }
        }
        try {
            psDelCategoria.setInt(1, c.getId());
            if (psDelCategoria.executeUpdate() == 0) {
                throw new GestorBDClub("No se ha eliminado ninguna categoría con el id especificado");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al eliminar la categoría", ex);
        }
    }

    @Override
    public void afegirEquip(Equip e) throws GestorBDClub {
        if (psInsertEquip == null) {
            try {
                psInsertEquip = conn.prepareStatement("INSERT INTO equip (nom, tipus, temporada, id_cat) VALUES (?, ?, ?, ?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psInsertEquip", ex);
            }
        }
        try {
            psInsertEquip.setString(1, e.getNom());
            psInsertEquip.setString(2, e.getTipus().name());
            psInsertEquip.setInt(3, e.getTemporada().getYear());
            psInsertEquip.setInt(4, e.getCategoria().getId());
            psInsertEquip.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al insertar el equipo", ex);
        }
    }

    @Override
    public void esborrarEquip(Equip e) throws GestorBDClub {
        if (psDelEquip == null) {
            try {
                psDelEquip = conn.prepareStatement("DELETE FROM equip WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psDelEquip", ex);
            }
        }
        try {
            psDelEquip.setInt(1, e.getId());
            if (psDelEquip.executeUpdate() == 0) {
                throw new GestorBDClub("No se ha eliminado ningún equipo con el id especificado");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al eliminar el equipo", ex);
        }
    }

    @Override
    public void modificarEquip(Equip e) throws GestorBDClub {
        if (psModificarEquip == null) {
            try {
                psModificarEquip = conn.prepareStatement("UPDATE equip SET nom = ? WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psModificarEquip", ex);
            }
        }
        try {
            psModificarEquip.setString(1, e.getNom());
            psModificarEquip.setInt(2, e.getId());

            if (psModificarEquip.executeUpdate() == 0) {
                throw new GestorBDClub("No se encontró el equipo para modificar");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al modificar el equipo", ex);
        }
    }

    @Override
    public void afegirMembre(Membre m) throws GestorBDClub {
        if (psInsertMembre == null) {
            try {
                psInsertMembre = conn.prepareStatement(
                    "INSERT INTO membre (jugador_id, equip_id, titular_convidat) VALUES (?, ?, ?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psInsertMembre", ex);
            }
        }
        try {
            psInsertMembre.setInt(1, m.getJ().getId());
            psInsertMembre.setInt(2, m.getE().getId());
            psInsertMembre.setString(3, String.valueOf(m.getTitular_convidat()));
            psInsertMembre.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en inserir membre", ex);
        }
    }

    @Override
    public void esborrarMembre(int id_j) throws GestorBDClub {
        if (psDelMembre == null) {
            try {
                psDelMembre = conn.prepareStatement("DELETE FROM membre WHERE jugador_id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psDelMembre", ex);
            }
        }
        try {
            psDelMembre.setInt(1, id_j);
            psDelMembre.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en esborrar membre", ex);
        }
    }

    @Override
    public void modificarMembre(Membre m) throws GestorBDClub {
        if (m == null) {
            throw new GestorBDClub("El membre no pot ser null");
        }
        
        if (psUpdateMembre == null) {
            try {
                psUpdateMembre = conn.prepareStatement(
                    "UPDATE membre SET titular_convidat = ? WHERE id_jugador = ? AND id_equip = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psUpdateMembre", ex);
            }
        }
        
        try {
            psUpdateMembre.setString(1, String.valueOf(m.getTitular_convidat()));
            psUpdateMembre.setInt(2, m.getJ().getId());
            psUpdateMembre.setInt(3, m.getE().getId());
            psUpdateMembre.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en modificar membre", ex);
        }
    }

    @Override
    public Membre obtenirMembre(int idJugador, int idEquip) throws GestorBDClub {
        if (psSelectMembre == null) {
            try {
                psSelectMembre = conn.prepareStatement(
                    "SELECT * FROM membre WHERE id_jugador = ? AND id_equip = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psSelectMembre", ex);
            }
        }
        
        try {
            psSelectMembre.setInt(1, idJugador);
            psSelectMembre.setInt(2, idEquip);
            ResultSet rs = psSelectMembre.executeQuery();
            
            if (rs.next()) {
                Jugador jugador = obtenirJugador(idJugador);
                Equip equip = obtenirEquip(idEquip);
                char titularConvidat = rs.getString("titular_convidat").charAt(0);
                
                return new Membre(jugador, equip, titularConvidat);
            }
            return null;
        } catch (SQLException | DataException ex) {
            throw new GestorBDClub("Error en obtenir membre", ex);
        }
    }

    @Override
    public List<Membre> obtenirLlistaMembre(int e) throws GestorBDClub {
        List<Membre> membres = new ArrayList<>();
        if (psSelectMembresByEquip == null) {
            try {
                psSelectMembresByEquip = conn.prepareStatement(
                    "SELECT m.*, j.*, e.* FROM membre m " +
                    "JOIN jugador j ON m.jugador_id = j.id " +
                    "JOIN equip e ON m.equip_id = e.id " +
                    "WHERE m.equip_id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psSelectMembresByEquip", ex);
            }
        }
        
        try {
            psSelectMembresByEquip.setInt(1, e);
            ResultSet rs = psSelectMembresByEquip.executeQuery();
            
            while (rs.next()) {
                Jugador jugador = new Jugador(
                    rs.getInt("j.id"),
                    rs.getString("j.nom"),
                    rs.getString("j.cognoms"),
                    Sexe.valueOf(rs.getString("j.sexe")),
                    rs.getDate("j.data_naix"),
                    rs.getString("j.idLegal"),
                    rs.getString("j.IBAN"),
                    rs.getString("j.adreça"),
                    rs.getString("j.poblacio"),
                    rs.getInt("j.cp"),
                    rs.getString("j.foto"),
                    rs.getInt("j.any_fi_revisió_mèdica")
                );
                
                Equip equip = obtenirEquip(e);
                
                char titularConvidat = rs.getString("m.titular_convidat").charAt(0);
                Membre membre = new Membre(jugador, equip, titularConvidat);
                
                membres.add(membre);
            }
            return membres;
        } catch (SQLException | DataException ex) {
            throw new GestorBDClub("Error en obtenir llista de membres", ex);
        }
    }

    @Override
    public void afegirUsuari(Usuari u) throws GestorBDClub {
        if (psInsertUsuari == null) {
            try {
                psInsertUsuari = conn.prepareStatement("INSERT INTO usuari (login, nom, password) VALUES (?, ?, ?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psInsertUsuari", ex);
            }
        }
        try {
            psInsertUsuari.setString(1, u.getLogin());
            psInsertUsuari.setString(2, u.getNom());
            psInsertUsuari.setString(3, u.getPassword());
            psInsertUsuari.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al insertar el usuario", ex);
        }
    }

    @Override
    public void esborrarUsuari(String login) throws GestorBDClub {
        if (psDelUsuari == null) {
            try {
                psDelUsuari = conn.prepareStatement("DELETE FROM usuari WHERE login = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psDelUsuari", ex);
            }
        }
        try {
            psDelUsuari.setString(1, login);
            if (psDelUsuari.executeUpdate() == 0) {
                throw new GestorBDClub("No se ha eliminado ningún usuario con el login especificado");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al eliminar el usuario", ex);
        }
    }

    @Override
    public Usuari getUsuari(String login) throws GestorBDClub {
        if (psObtenirUsuari == null) {
            try {
                psObtenirUsuari = conn.prepareStatement(
                    "SELECT * FROM Usuari WHERE login = ?"
                );
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar la sentència psObtenirUsuari", ex);
            }
        }

        try {
            psObtenirUsuari.setString(1, login);
            try (ResultSet rs = psObtenirUsuari.executeQuery()) {
                if (rs.next()) {
                    return new Usuari(
                        rs.getString("login"),
                        rs.getString("nom"),
                        rs.getString("password")
                    );
                } else {
                    throw new GestorBDClub("Usuari no trobat amb login: " + login);
                }
            } catch (DataException ex) {
                throw new GestorBDClub("Error en convertir dades", ex);
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en obtenir un usuari", ex);
        }
    }
    @Override
    public void modificarUsuari(Usuari u) throws GestorBDClub {
        if (psModificarUsuari == null) {
            try {
                psModificarUsuari = conn.prepareStatement("UPDATE usuari SET nom = ?, password = ? WHERE login = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psModificarUsuari", ex);
            }
        }
        try {
            psModificarUsuari.setString(1, u.getNom());
            psModificarUsuari.setString(2, u.getPassword());
            psModificarUsuari.setString(3, u.getLogin());

            if (psModificarUsuari.executeUpdate() == 0) {
                throw new GestorBDClub("No se encontró el usuario para modificar");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al modificar el usuario", ex);
        }
    }

    @Override
    public List<Jugador> obtenirLlistaJugador() throws GestorBDClub {
        List<Jugador> jugadores = new ArrayList<>();
        Statement q = null;
        try {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM jugador");
            while (rs.next()) {                
                Jugador j = new Jugador(
                    rs.getInt("id"),
                    rs.getString("nom"),
                    rs.getString("cognoms"),
                    Sexe.valueOf(rs.getString("sexe")),
                    rs.getDate("data_naix"),
                    rs.getString("idLegal"),
                    rs.getString("IBAN"),
                    rs.getString("adreça"),
                    rs.getString("poblacio"),
                    rs.getInt("cp"),
                    rs.getString("foto"),
                    rs.getInt("any_fi_revisió_mèdica")
                );
                jugadores.add(j);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al obtener la lista de jugadores", ex);
        } catch (DataException ex) {
            throw new GestorBDClub("Data incorrecte trobat o corrupte! ", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDClub("Error al cerrar la sentencia", ex);
                }
            }
        }
        return jugadores;
    }

    @Override
    public List<Temporada> obtenirLlistaTemporada() throws GestorBDClub {
        List<Temporada> temporadas = new ArrayList<>();
        Statement q = null;
        try {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM temporada");
            while (rs.next()) {
                Temporada temporada = new Temporada(
                    rs.getInt("year")
                );
                temporadas.add(temporada);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al obtener la lista de temporadas", ex);
        } catch (DataException ex) {
            throw new GestorBDClub("Data incorrecte", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDClub("Error al cerrar la sentencia", ex);
                }
            }
        }
        return temporadas;
    }


    @Override
    public List<Categoria> obtenirLlistaCategoria() throws GestorBDClub {
        List<Categoria> categorias = new ArrayList<>();
        Statement q = null;
        try {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM categoria");

            while (rs.next()) {
                Categoria c = new Categoria(
                    rs.getInt("id"),
                    rs.getString("nom"),
                    rs.getInt("edat_min"),
                    rs.getInt("edat_max")
                );
                categorias.add(c);
            }

            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al obtener la lista de categorías", ex);
        } catch (DataException ex) {
            throw new GestorBDClub("Datos incorrectos o corruptos en la categoría", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDClub("Error al cerrar la sentencia", ex);
                }
            }
        }
        return categorias;
    }

    @Override
    public boolean validarLogin(String login, String password) throws GestorBDClub {
        if (psValidarLogin == null) {
            try {
                psValidarLogin = conn.prepareStatement("SELECT * from usuari WHERE login = ? and password = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psInsertUsuari", ex);
            }
        }
        try {
            psValidarLogin.setString(1, login);
            psValidarLogin.setString(2, password);
            //System.out.println("Contraseña ingresada: " + password);
            ResultSet rs = psValidarLogin.executeQuery();
            if (rs.next()) {
                //System.out.println(rs.getString("password"));
                return true; 
            } else {
                return false; 
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al intentar validar usuario", ex);
        }
    }    

    @Override
    public List<Equip> obtenirLlistaEquip() throws GestorBDClub {
        List<Equip> equips = new ArrayList<>();
        Statement q = null;
        try {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM equip");
            while (rs.next()) {                
                Equip e = new Equip(
                    rs.getInt("id"),
                    rs.getString("nom"),
                Tipus.valueOf(rs.getString("tipus")),
            getCategoria(rs.getInt("id_cat")),
            getTemporada(rs.getInt("temporada"))
                );
                equips.add(e);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al obtener la lista de equips", ex);
        } catch (DataException ex) {
            throw new GestorBDClub("Error al obtener la lista de jugadores en transformar alguna dada", ex);
        } finally {
            if (q != null) {
                try {
                    q.close();
                } catch (SQLException ex) {
                    throw new GestorBDClub("Error al cerrar la sentencia", ex);
                }
            }
        }
        return equips;
    }

    public Categoria getCategoria(int id) throws GestorBDClub{
        if (psObtenirCategoria == null) {
            try {
                psObtenirCategoria = conn.prepareStatement(
                    "SELECT * FROM categoria WHERE id = ?"
                );
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar la sentència psObtenirCategoria", ex);
            }
        }

        try {
            psObtenirCategoria.setInt(1, id);
            try (ResultSet rs = psObtenirCategoria.executeQuery()) {
                if (rs.next()) {
                    return new Categoria(
                        id,   
                        rs.getString("nom"),
                        rs.getInt("edat_min"),
                        rs.getInt("edat_max")
                    );
                } else {
                    throw new GestorBDClub("Categoria no trobat amb id: " + id);
                }
            } catch (DataException ex) {
                throw new GestorBDClub("Error en convertir dades", ex);
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en obtenir una categoria", ex);
        }
    }
    public Temporada getTemporada(int year) throws GestorBDClub{
        if (psObtenirTemporada == null) {
            try {
                psObtenirTemporada = conn.prepareStatement(
                    "SELECT * FROM temporada WHERE year = ?"
                );
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar la sentència psObtenirTemporada", ex);
            }
        }

        try {
            psObtenirTemporada.setInt(1, year);
            try (ResultSet rs = psObtenirTemporada.executeQuery()) {
                if (rs.next()) {
                    return new Temporada(
                        year
                    );
                } else {
                    throw new GestorBDClub("Temporada no trobat amb any: " + year);
                }
            } catch (DataException ex) {
                throw new GestorBDClub("Error en convertir dades", ex);
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en obtenir una temporada", ex);
        }
    }

    private Equip obtenirEquip(int id) throws GestorBDClub, SQLException {
        if (psSelectEquip == null) {
            psSelectEquip = conn.prepareStatement(
                "SELECT * FROM equip WHERE id = ?");
        }
        
        psSelectEquip.setInt(1, id);
        ResultSet rs = psSelectEquip.executeQuery();
        
        if (rs.next()) {
            try {
                return new Equip(
                    rs.getInt("id"),
                    rs.getString("nom"),
                    Tipus.valueOf(rs.getString("tipus")),
                    getCategoria(rs.getInt("id_cat")),
                    getTemporada(rs.getInt("temporada"))
                );
            } catch (DataException ex) {
                throw new GestorBDClub("Error al crear equip", ex);
            }
        }
        return null;
    }
    @Override
    public Jugador obtenirJugador(int id) throws GestorBDClub {
        Jugador cachedJugador = jugadorCache.get(id);
        if (cachedJugador != null) {
            return cachedJugador;
        }

        if (psSelectJugador == null) {
            try {
                psSelectJugador = conn.prepareStatement(
                    "SELECT * FROM jugador WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentència psSelectJugador", ex);
            }
        }
        
        try {
            psSelectJugador.setInt(1, id);
            ResultSet rs = psSelectJugador.executeQuery();
            
            if (rs.next()) {
                Jugador jugador = new Jugador(
                    rs.getInt("id"),
                    rs.getString("nom"),
                    rs.getString("cognoms"),
                    Sexe.valueOf(rs.getString("sexe")),
                    rs.getDate("data_naix"),
                    rs.getString("idLegal"),
                    rs.getString("IBAN"),
                    rs.getString("adreça"),
                    rs.getString("poblacio"),
                    rs.getInt("cp"),
                    rs.getString("foto"),
                    rs.getInt("any_fi_revisió_mèdica")
                );
                jugadorCache.put(id, jugador);
                return jugador;
            }
            return null;
        } catch (SQLException | DataException ex) {
            throw new GestorBDClub("Error en obtenir jugador", ex);
        }
    }
    
    //FILES
    @Override
    public void exportJugadorsToXML(File file) throws GestorBDClub {
        try {
            Element root = new Element("jugadors");
            Document doc = new Document(root);
            
            List<Jugador> jugadors = obtenirLlistaJugador();
            for (Jugador j : jugadors) {
                Element jugadorElement = new Element("jugador");
                jugadorElement.setAttribute("id", String.valueOf(j.getId()));
                
                jugadorElement.addContent(new Element("nom").setText(j.getNom()));
                jugadorElement.addContent(new Element("cognoms").setText(j.getCognom()));
                jugadorElement.addContent(new Element("sexe").setText(j.getSexeString()));
                jugadorElement.addContent(new Element("dataNaixement")
                    .setText(new SimpleDateFormat("dd/MM/yyyy").format(j.getData_naix())));
                jugadorElement.addContent(new Element("idLegal").setText(j.getIdLegal()));
                jugadorElement.addContent(new Element("iban").setText(j.getIban()));
                jugadorElement.addContent(new Element("adreca").setText(j.getAdresa()));
                jugadorElement.addContent(new Element("poblacio").setText(j.getPoblacio()));
                jugadorElement.addContent(new Element("cp").setText(String.valueOf(j.getCp())));
                jugadorElement.addContent(new Element("foto").setText(j.getFoto()));
                jugadorElement.addContent(new Element("anyFiRevisio")
                    .setText(String.valueOf(j.getAny_fi_revisio_medica())));
                
                root.addContent(jugadorElement);
            }
            
            writeXMLToFile(doc, file);
        } catch (Exception ex) {
            throw new GestorBDClub("Error al exportar jugadors a XML", ex);
        }
    }

    @Override
    public void exportEquipsToXML(File file) throws GestorBDClub {
        try {
            Element root = new Element("equips");
            Document doc = new Document(root);
            
            List<Equip> equips = obtenirLlistaEquip();
            for (Equip e : equips) {
                Element equipElement = new Element("equip");
                equipElement.setAttribute("id", String.valueOf(e.getId()));
                
                equipElement.addContent(new Element("nom").setText(e.getNom()));
                equipElement.addContent(new Element("tipus").setText(e.getTipus().toString()));
                equipElement.addContent(new Element("categoria").setText(e.getCategoria().getCategoria()));
                equipElement.addContent(new Element("temporada")
                    .setText(String.valueOf(e.getTemporada().getYear())));
                
                root.addContent(equipElement);
            }
            
            writeXMLToFile(doc, file);
        } catch (Exception ex) {
            throw new GestorBDClub("Error al exportar equips a XML", ex);
        }
    }

    @Override
    public void exportMembresToXML(File file) throws GestorBDClub {
        try {
            Element root = new Element("membres");
            Document doc = new Document(root);
            
            List<Equip> equips = obtenirLlistaEquip();
            for (Equip e : equips) {
                List<Membre> membres = obtenirLlistaMembre(e.getId());
                for (Membre m : membres) {
                    Jugador j = obtenirJugador(m.getJ().getId());
                    Element membreElement = new Element("membre");
                    
                    membreElement.addContent(new Element("equip").setText(e.getNom()));
                    membreElement.addContent(new Element("jugador")
                        .setText(j.getNom() + " " + j.getCognom()));
                    membreElement.addContent(new Element("tipus")
                        .setText(String.valueOf(m.getTitular_convidat())));
                    
                    root.addContent(membreElement);
                }
            }
            
            writeXMLToFile(doc, file);
        } catch (Exception ex) {
            throw new GestorBDClub("Error al exportar membres a XML", ex);
        }
    }

    private void writeXMLToFile(Document doc, File file) throws IOException {
        XMLOutputter xmlOutput = new XMLOutputter();
        xmlOutput.setFormat(Format.getPrettyFormat());
        xmlOutput.output(doc, new FileWriter(file));
    }
    
    @Override
    public void exportJugadorsToCSV(File file) throws GestorBDClub {
        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println("ID,Nom,Cognoms,Sexe,Data Naixement,ID Legal,IBAN,Adreça,Població,CP,Foto,Any Fi Revisió");

            List<Jugador> jugadors = obtenirLlistaJugador();
            for (Jugador j : jugadors) {
                writer.printf("%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%d%n",
                    j.getId(),
                    escaparCSV(j.getNom()),
                    escaparCSV(j.getCognom()),
                    j.getSexeString(),
                    new SimpleDateFormat("dd/MM/yyyy").format(j.getData_naix()),
                    escaparCSV(j.getIdLegal()),
                    escaparCSV(j.getIban()),
                    escaparCSV(j.getAdresa()),
                    escaparCSV(j.getPoblacio()),
                    j.getCp(),
                    escaparCSV(j.getFoto()),
                    j.getAny_fi_revisio_medica()
                );
            }
        } catch (IOException ex) {
            throw new GestorBDClub("Error al exportar jugadors a CSV", ex);
        }
    }

    @Override
    public void exportEquipsToCSV(File file) throws GestorBDClub {
        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println("ID,Nom,Tipus,Categoria,Temporada");
            
            List<Equip> equips = obtenirLlistaEquip();
            for (Equip e : equips) {
                writer.printf("%d,%s,%s,%s,%d%n",
                    e.getId(),
                    escaparCSV(e.getNom()),
                    e.getTipus(),
                    escaparCSV(e.getCategoria().getCategoria()),
                    e.getTemporada().getYear()
                );
            }
        } catch (IOException ex) {
            throw new GestorBDClub("Error al exportar equips a CSV", ex);
        }
    }

    @Override
    public void exportMembresToCSV(File file) throws GestorBDClub {
        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println("Equip,Jugador,Tipus");
            
            List<Equip> equips = obtenirLlistaEquip();
            for (Equip e : equips) {
                List<Membre> membres = obtenirLlistaMembre(e.getId());
                for (Membre m : membres) {
                    Jugador j = obtenirJugador(m.getJ().getId());
                    writer.printf("%s,%s %s,%c%n",
                        escaparCSV(e.getNom()),
                        escaparCSV(j.getNom() + " " + j.getCognom()),
                        m.getTitular_convidat()
                    );
                }
            }
        } catch (IOException ex) {
            throw new GestorBDClub("Error al exportar membres a CSV", ex);
        }
    }

    private String escaparCSV(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
    
    private void clearCaches() {
        jugadorCache.clear();
        membreCache.clear();
        equipCache.clear();
        temporadaCache.clear();
        categoriaCache.clear();
    }
}
