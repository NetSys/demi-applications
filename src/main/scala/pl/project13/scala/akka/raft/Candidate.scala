package pl.project13.scala.akka.raft

import model._
import protocol._
import config.RaftConfig
import akka.actor.ActorRef


private[raft] trait Candidate {
  this: RaftActor =>

  protected def raftConfig: RaftConfig

  val candidateBehavior: StateFunction = {
    // message from client, tell it that we know of no leader
    case Event(msg: ClientMessage[Command], m: ElectionMeta) =>
      log.info("Candidate got {} from client; Respond with anarchy - there is no leader.", msg)
      sender() ! LeaderIs(None, Some(msg))
      stay()

    // election
    case Event(BeginElection, m: ElectionMeta) =>
      if (m.config.members.isEmpty) {
        log.warning("Tried to initialize election with no members...")
        goto(Follower) using m.forFollower()
      } else {
        log.info("Initializing election (among {} nodes) for {}", m.config.members.size, m.currentTerm)

        val request = RequestVote(m.currentTerm, m.clusterSelf, replicatedLog.lastTerm, replicatedLog.lastIndex)
        m.membersExceptSelf foreach { _ ! request }

        if (m.votes contains m.currentTerm) {
          // We already voted for ourself this Term!
          stay()
        } else {
          val includingThisVote = m.incVote(self)
          stay() using includingThisVote.withVoteFor(m.currentTerm, m.clusterSelf)
        }
      }

    case Event(msg: RequestVote, m: ElectionMeta) if msg.term < m.currentTerm =>
      log.info("Rejecting RequestVote msg by {} in {}. Received stale {}.", candidate, m.currentTerm, msg.term)
      candidate ! DeclineCandidate(m.currentTerm)
      stay()

    case Event(msg: RequestVote, m: ElectionMeta) if msg.term > m.currentTerm =>
      log.info("Received newer {}. Current term is {}. Revert to follower state.", msg.term, m.currentTerm)
      m.clusterSelf forward msg
      goto(Follower) using m.forFollower(msg.term)

    case Event(msg: RequestVote, m: ElectionMeta)
      if m.canVoteIn(msg.term, msg.candidateId) =>
      // If we're going to vote for someone other than ourselves this term,
      // step down to Follower.
      m.clusterSelf forward msg
      goto(Follower) using m.forFollower(msg.term)

    case Event(msg: RequestVote, m: ElectionMeta) =>
      sender ! DeclineCandidate(msg.term)
      stay()

    case Event(VoteCandidate(term), m: ElectionMeta) if term < m.currentTerm =>
      log.info("Rejecting VoteCandidate msg by {} in {}. Received stale {}.", voter(), m.currentTerm, term)
      voter ! DeclineCandidate(m.currentTerm)
      stay()

   case Event(VoteCandidate(term), m: ElectionMeta) if term > m.currentTerm =>
     log.info("Received newer {}. Current term is {}. Revert to follower state.", term, m.currentTerm)
     goto(Follower) using m.forFollower(term)

   case Event(VoteCandidate(term), m: ElectionMeta) =>
      val includingThisVote = m.incVote(voter())

      if (includingThisVote.hasMajority) {
        log.info("Received vote by {}; Won election with {} of {} votes", voter(), includingThisVote.votesReceived, m.config.members.size)
        LeaderTest.totalElected.getAndIncrement // XXX STS2 testing

        val leaderState = m.forLeader
        initializeLeaderState(leaderState.config.members, leaderState)
        goto(Leader) using leaderState
      } else {
        log.info("Received vote by {}; Have {} of {} votes", voter(), includingThisVote.votesReceived, m.config.members.size)
        stay() using includingThisVote
      }

    case Event(DeclineCandidate(term), m: ElectionMeta) if term > m.currentTerm =>
      log.info("Received newer {}. Current term is {}. Revert to follower state.", term, m.currentTerm)
      goto(Follower) using m.forFollower(term)
      stay()

    case Event(DeclineCandidate(term), m: ElectionMeta) =>
      log.info("Rejected vote by {}, in term {}", voter(), term)
      stay()

    // end of election

    // handle appends
    case Event(append: AppendEntries[Entry[Command]], m: ElectionMeta) =>
      val leaderIsAhead = append.term >= m.currentTerm

      if (leaderIsAhead) {
        log.info("Reverting to Follower, because got AppendEntries from Leader in {}, but am in {}", append.term, m.currentTerm)
        m.clusterSelf forward append
        goto(Follower) using m.forFollower()
      } else {
        stay()
      }

    // ending election due to timeout
    case Event(ElectionTimeout, m: ElectionMeta) if m.config.members.size > 1 =>
      log.info("Voting timeout, starting a new election (among {})...", m.config.members.size)
      m.clusterSelf ! BeginElection
      resetElectionDeadline()
      stay() using m.forNewElection

    // would like to start election, but I'm all alone! ;-(
    case Event(ElectionTimeout, m: ElectionMeta) =>
      log.info("Voting timeout, unable to start election, don't know enough nodes (members: {})...", m.config.members.size)
      goto(Follower) using m.forFollower()

    case Event(AskForState, _) =>
      sender() ! IAmInState(Candidate)
      stay()
  }

  // Invoked before transitioning to Leader.
  def initializeLeaderState(members: Set[ActorRef], m: LeaderMeta) {
    log.info("Preparing nextIndex and matchIndex table for followers.")
    nextIndex = LogIndexMap.initialize(members, replicatedLog.nextIndex)
    matchIndex = LogIndexMap.initialize(members, 0)
    matchIndex.put(m.clusterSelf, replicatedLog.lastIndex)
  }
}
