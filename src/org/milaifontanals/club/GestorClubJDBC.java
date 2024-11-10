/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.milaifontanals.club;

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
            throw new GestorBDClub("Error en confirmar canvis", ex);
        }
    }

    @Override
    public void afegirJugador(Jugador j) throws GestorBDClub {
        if (psInsertJugador == null) {
            try {
                psInsertJugador = conn.prepareStatement("INSERT INTO jugador (nom, cognoms, data_naix, sexe, adreça, foto, any_fi_revisió_mèdica, IBAN, idLegal) "
                                                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
            
            psInsertJugador.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en intentar inserir el Jugador " + j.getNom(), ex);
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
                
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en eliminar el jugador ", ex);
        }
    }

    
    @Override
    public void desferCanvis() throws GestorBDClub {
        try {
            conn.rollback();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error en desfer canvis", ex);
        }
    }
    
    @Override
    public void modificarJugador(Jugador j) throws GestorBDClub {
        if (psModificarJugador == null) {
            try {
                psModificarJugador = conn.prepareStatement("UPDATE jugador SET nom = ?, cognoms = ?, data_naix = ?, sexe = ?, adreça = ?, foto = ?, any_fi_revisió_mèdica = ?, IBAN = ?, idLegal = ? WHERE id = ?");
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
            psModificarJugador.setInt(10, j.getId());
            
            if (psModificarJugador.executeUpdate() == 0) {
                throw new GestorBDClub("No se encontró el jugador para modificar");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al modificar el jugador", ex);
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
            psInsertEquip.setString(1, e.getNom());
            psModificarEquip.setInt(5, e.getId());

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
                psInsertMembre = conn.prepareStatement("INSERT INTO membre (equip_id, jugador_id, titular_convidat) VALUES (?, ?, ?)");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psInsertMembre", ex);
            }
        }
        try {
            psInsertMembre.setInt(1, m.getE().getId());
            psInsertMembre.setInt(2, m.getJ().getId());
            psInsertMembre.setString(3, m.getTitular_convidat()+"");
            psInsertMembre.executeUpdate();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al insertar el miembro", ex);
        }
    }

    @Override
    public void esborrarMembre(int id_j) throws GestorBDClub {
        if (psDelMembre == null) {
            try {
                psDelMembre = conn.prepareStatement("DELETE FROM membre WHERE jugador_id = ?");
            } catch (SQLException ex) {
                throw new GestorBDClub("Error en preparar sentencia psDelMembre", ex);
            }
        }
        try {
            psDelMembre.setInt(1, id_j);
            if (psDelMembre.executeUpdate() == 0) {
                throw new GestorBDClub("No se ha eliminado ningún miembro con el id del jugador especificado");
            }
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al eliminar el miembro", ex);
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
    public List<Membre> obtenirLlistaMembre(Equip e) throws GestorBDClub {
        List<Membre> membres = new ArrayList<>();
        Statement stmt = null;
        try {
            // Crear la sentencia para obtener los miembros de un equipo
            stmt = conn.createStatement();

            // Consulta SQL para obtener los miembros por el id del equipo
            String query = "SELECT j.id AS jugador_id, j.nom, j.cognoms, j.sexe, j.data_naix, j.idLegal, j.IBAN, j.adreça, j.foto, j.any_fi_revisió_mèdica, "
                         + "m.titular_convidat "
                         + "FROM membre m "
                         + "JOIN jugador j ON m.jugador_id = j.id "
                         + "WHERE m.equip_id = " + e.getId();

            // Ejecutar la consulta SQL
            ResultSet rs = stmt.executeQuery(query);

            // Procesar el resultado de la consulta
            while (rs.next()) {
                // Crear un objeto Jugador desde los resultados
                Jugador j = new Jugador(
                    rs.getInt("jugador_id"),
                    rs.getString("nom"),
                    rs.getString("cognoms"),
                    Sexe.valueOf(rs.getString("sexe")),
                    rs.getDate("data_naix"),
                    rs.getString("idLegal"),
                    rs.getString("IBAN"),
                    rs.getString("adreça"),
                    rs.getString("foto"),
                    rs.getInt("any_fi_revisió_mèdica")
                );

                // Crear un objeto Membre con el jugador, el equipo y el tipo (titular o convidat)
                Membre membre = new Membre(j, e, rs.getString("titular_convidat").charAt(0));
                membres.add(membre);
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDClub("Error al obtener la lista de miembros", ex);
        } catch (DataException ex) {
            throw new GestorBDClub("Error de datos al procesar los miembros", ex);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    throw new GestorBDClub("Error al cerrar la sentencia", ex);
                }
            }
        }
        return membres;
    }

}
