//
// Copyright (c) 2023, SkyFoundry LLC
// All Rights Reserved
//
// History:
//   20 Mar 2024  Mike Jarmy  Creation
//

package fan.xbdPostgres;

import fan.sys.FanObj;
import fan.sys.Type;
import fan.sys.List;

import java.sql.*;
import java.util.Properties;

public class PostgresDb extends FanObj
{

//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  public static PostgresDb make()
  {
    return new PostgresDb();
  }

  public final Type typeof() { return typeof; }
  private static final Type typeof = Type.find("xbdPostgres::PostgresDb");

//////////////////////////////////////////////////////////////////////////
// Native
//////////////////////////////////////////////////////////////////////////

  public void open(String uri, String username, String password)
      throws Exception
  {
    Properties props = new Properties();
    props.setProperty("user", username);
    props.setProperty("password", password);
    this.conn = DriverManager.getConnection(uri, props);
    conn.setAutoCommit(false);

    this.insertRec = conn.prepareStatement(
      "insert into rec (id, paths, values_, refs, units, spec) " +
      "values (?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?)");

    this.insertPathRef = conn.prepareStatement(
      "insert into pathRef (rec_id, path_, ref_) values (?, ?, ?)");

    this.insertSpec = conn.prepareStatement(
      "insert into spec (qname, inherits_from) values (?, ?)");
  }

  public void close() throws Exception
  {
    insertRec.close();
    insertPathRef.close();
    insertSpec.close();
    conn.close();

    insertRec = null;
    insertPathRef = null;
    insertSpec = null;
    conn = null;
  }

  public void writeSpec(String name, List inherits)
    throws Exception
  {
    insertSpec.setString(1, name);
    insertSpec.setObject(2, toStringArray(inherits));
    insertSpec.executeUpdate();
    conn.commit();
  }

  public void writeRec(DbRec rec, List pathRefs)
    throws Exception
  {
    insertRec.setString(1, rec.id);
    insertRec.setObject(2, toStringArray(rec.paths));
    insertRec.setString(3, rec.values);
    insertRec.setString(4, rec.refs);
    insertRec.setString(5, rec.units);
    insertRec.setString(6, rec.spec);
    insertRec.executeUpdate();

    for (int i = 0; i < pathRefs.size(); i++)
    {
      PathRef p = (PathRef) pathRefs.get(i);
      insertPathRef.setString(1, rec.id);
      insertPathRef.setString(2, p.path);
      insertPathRef.setString(3, p.ref);
      insertPathRef.addBatch();
    }
    insertPathRef.executeBatch();

    conn.commit();
  }

//  public List query(Query query)
//    throws Exception
//  {
//    List result = new List(Type.find("xbdPostgres::DbRec"));
//
//    // prepare
//    try (PreparedStatement stmt = conn.prepareStatement(query.sql)) {
//
//      // set params
//      for (int i = 0; i < query.params.size(); i++)
//        stmt.setString(i+1, (String) query.params.get(i));
//
//      // execute
//      try (ResultSet rs = stmt.executeQuery()) {
//
//        // for each
//        while(rs.next()) {
//          // TODO populate other fields
//          result.add(DbRec.make(
//            rs.getString(1),
//            new List(Type.find("sys::Str")),
//            new List(Type.find("xbdPostgres::PathRef")),
//            "",
//            "",
//            null));
//        }
//      }
//    }
//
//    return result;
//  }

  private static String[] toStringArray(List list)
  {
    String[] arr = new String[(int)list.size()];
    for (int i = 0; i < list.size(); i++)
      arr[i] = (String) list.get(i);
    return arr;
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private Connection conn;
  private PreparedStatement insertRec;
  private PreparedStatement insertPathRef;
  private PreparedStatement insertSpec;
}

