package test

import "testing"

func Test_ReadCommitted(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)
	conn, client, session1 := createTestClient(t)
	session2 := InitNewClient(t, client)
	// set two keys. cannot read uncommitted.
	key1 := "1"
	val1 := "1"
	Begin(t, client, session1)
	Set(t, client, session1, []string{key1, val1}, "")
	Rollback(t, client, session1)
	Get(t, client, session2, []string{key1}, "", keyNotFound)
	closeServerClient(t, cancel, conn, listener)

	// Restart, read committed.
	cancel, listener = startTestServer(t)
	conn, client, session1 = createTestClient(t)
	session2 = InitNewClient(t, client)
	Set(t, client, session1, []string{key1, val1}, "")
	Get(t, client, session2, []string{key1}, val1, "")
	closeServerClient(t, cancel, conn, listener)
}

//func Test_RepeatableRead(t *testing.T) {
//	InitLog(t)
//	cancel, listener := startTestServer(t)
//	defer clearTestData(t)
//	conn, client, session1 := createTestClient(t)
//	session2 := InitNewClient(t, client)
//	// set two keys. cannot read uncommitted.
//	key1 := "1"
//	val1 := "1"
//	Set(t, client, session1, []string{key1, val1}, "")
//	Begin(t, client, session2)
//	Get(t, client, session2, []string{key1}, val1, "")
//	Begin(t, client, session1)
//	Del(t, client, session1, []string{key1}, "")
//
//	Get(t, client, session2, []string{key1}, val1, "")
//	closeServerClient(t, cancel, conn, listener)
//}
