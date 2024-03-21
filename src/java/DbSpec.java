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

public class DbSpec extends FanObj
{

//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  public static DbSpec make()
  {
    DbSpec self = new DbSpec();
    //make$(self, arg);
    return self;
  }

  public final Type typeof() { return typeof; }
  private static final Type typeof = Type.find("xbdPostgres::DbSpec");

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
    //conn.setAutoCommit(false);

    this.insertSpec = conn.prepareStatement(
      "insert into spec (qname) values (?)");
  }

  public void close() throws Exception
  {
    insertSpec.close();
    conn.close();
  }

  public void writeSpec(String name) throws Exception
  {
    System.out.println("writeSpec: " + name);
    insertSpec.setString(1, name);
    insertSpec.executeUpdate();
    //conn.commit();
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private Connection conn;
  private PreparedStatement insertSpec;
}

