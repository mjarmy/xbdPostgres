//
// Copyright (c) 2023, SkyFoundry LLC
// All Rights Reserved
//
// History:
//   20 Mar 2024  Mike Jarmy  Creation
//

package fan.xbdPostgres;

import fan.sys.*;

import java.sql.*;
import java.util.*;

public class PostgresDb extends FanObj
{

//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  public static PostgresDb make()
  {
    PostgresDb self = new PostgresDb();
    //make$(self, arg);
    return self;
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
      "insert into rec (id, paths, values_, units, spec) " +
      "values (?, ?, ?::jsonb, ?::jsonb, ?)");

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
  }

  public void writeSpec(String name, fan.sys.List inherits)
    throws Exception
  {
    insertSpec.setString(1, name);
    insertSpec.setObject(2, toStringArray(inherits));
    insertSpec.executeUpdate();
    conn.commit();
  }

  public void writeRec(
      String id,
      fan.sys.List paths,
      fan.sys.List pathRefs,
      String values, // json
      String units,  // json
      String spec)
    throws Exception
  {
    insertRec.setString(1, id);
    insertRec.setObject(2, toStringArray(paths));
    insertRec.setString(3, values);
    insertRec.setString(4, units);
    insertRec.setString(5, spec);
    insertRec.executeUpdate();

    for (int i = 0; i < pathRefs.size(); i++)
    {
      PathRef p = (PathRef) pathRefs.get(i);
      insertPathRef.setString(1, id);
      insertPathRef.setString(2, p.path);
      insertPathRef.setString(3, p.ref);
      insertPathRef.addBatch();
    }
    insertPathRef.executeBatch();

    conn.commit();
  }

  private static String[] toStringArray(fan.sys.List list)
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

