//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   5 June 2024  Mike Jarmy  Creation
//

using concurrent
using sql

**************************************************************************
** SqlConnPool
**************************************************************************

// TODO
//
// connectionTimeout This property controls the maximum number of milliseconds
// that a client will wait for a connection from the pool. If this time is
// exceeded without a connection becoming available, an Err will be thrown.
//
// keepAliveTime: This property controls how frequently we will attempt to keep
// a connection alive, in order to prevent it from being timed out by the
// database or network infrastructure

// maxLifetime: This property controls the maximum lifetime of a connection in
// the pool. An in-use connection will never be retired, only when it is closed
// will it then be removed

class SqlConnPool
{
  new make(
    Str uri, Str username, Str password,
    Int poolSize)
  {
    this.poolSize = poolSize
    conns = SqlConn[,]
    available = Bool[,]
    for (i := 0; i < poolSize; i++)
    {
      conns.add(SqlConn.open(uri, username, password))
      available.add(true)
    }

    this.actor = Actor(actorPool) |msg| { receive(msg) }
  }

  Void invoke(|SqlConn| func)
  {
    conn := actor.send(SqlConnPoolMsg(SqlConnPoolMsg.obtain)).get()
    func(conn)
    actor.send(SqlConnPoolMsg(SqlConnPoolMsg.release, conn))
  }

  Void shutdown()
  {
    actor.send(SqlConnPoolMsg(SqlConnPoolMsg.shutdown))
  }

  private Obj? receive(SqlConnPoolMsg? msg)
  {
    switch (msg.id)
    {
      case SqlConnPoolMsg.obtain:   return doObtain
      case SqlConnPoolMsg.release:  return doRelease(msg.a)
      case SqlConnPoolMsg.shutdown: return doShutdown()
      default:                      throw Err("Unknown msg: $msg")
    }
    return null
  }

  private SqlConn doObtain()
  {
    for (i := 0; i < poolSize; i++)
    {
      if (available[i])
      {
        available[i] = false
        return conns[i]
      }
    }

    // TODO retyr until connectionTimeout
    throw SqlConnPoolErr("No connection is available")
  }

  private Obj? doRelease(SqlConn conn)
  {
    for (i := 0; i < poolSize; i++)
    {
      if (conn === conns[i])
      {
        available[i] = true
        return null
      }
    }

    throw SqlConnPoolErr("SqlConn not found in pool")
  }

  private Obj? doShutdown()
  {
    for (i := 0; i < poolSize; i++)
    {
      try
      {
        conns[i].close
      }
      catch (Err e) {}
    }

    return null
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private static const ActorPool actorPool := ActorPool()
  private const Actor actor

  private const Int poolSize
  private SqlConn[] conns
  private Bool[] available
}

**************************************************************************
** SqlConnPoolMsg
**************************************************************************

internal const class SqlConnPoolMsg
{
  const static Int obtain   := 0  //
  const static Int release  := 1  // a=SqlConn
  const static Int shutdown := 2  //

  new make(Int id, Obj? a := null) { this.id = id; this.a = a }

  const Int id
  const Obj? a
}

**************************************************************************
** SqlConnPoolErr
**************************************************************************

const class SqlConnPoolErr : Err {
  new make(Str msg, Err? cause := null) : super(msg, cause) {}
}
