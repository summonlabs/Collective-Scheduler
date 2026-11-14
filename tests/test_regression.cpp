// Regression tests for grant accounting, dispatch exclusion and revocation.
#include <collective_scheduler/scheduler.hpp>
#include "test_common.hpp"
#include "test_framework.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

using namespace collective_scheduler;
using namespace tdata;

namespace {
// Fabric that records what it is handed and revoked, can refuse grants, and can
// call back into the scheduler once while deciding (ports run outside its lock).
class ScriptedFabric final : public CollectiveFabricPort {
 public:
  bool accept(const CollectiveGrant& g) override {
    accepted.push_back(g.id.value);
    if (on_accept) { auto f = std::move(on_accept); on_accept = nullptr; f(g); }
    return accept_result;
  }
  void revoke(const CollectiveGrant& g) override { revoked.push_back(g.id.value); }
  bool was_revoked(std::uint64_t id) const { return std::find(revoked.begin(), revoked.end(), id) != revoked.end(); }
  std::vector<std::uint64_t> accepted;
  std::vector<std::uint64_t> revoked;
  bool accept_result = true;
  std::function<void(const CollectiveGrant&)> on_accept;
};

Scheduler::Options Opts(std::shared_ptr<ScriptedFabric> fabric, std::size_t max_active = 4096) {
  Scheduler::Options o;
  o.policy.bounds.max_active_grants = max_active;
  o.fabric = std::move(fabric);
  return o;
}

std::vector<ParticipantGeneration> G2() { return {ParticipantGeneration(1), ParticipantGeneration(1)}; }

// Participants 1,2 on worker 1 (boot 11); 3,4 on worker 2 (boot 21); 5,6 on worker 3 (boot 31).
// Plans: 1 = {1,2}, 2 = {3,4}, 3 = {5,6}, 4 = {2,3} (shares 2 with plan 1).
void Populate(Scheduler& s) {
  s.publishParticipant(P(1, 1, 1, 11)); s.publishParticipant(P(2, 1, 1, 11));
  s.publishParticipant(P(3, 1, 2, 21)); s.publishParticipant(P(4, 1, 2, 21));
  s.publishParticipant(P(5, 1, 3, 31)); s.publishParticipant(P(6, 1, 3, 31));
  s.publishPlan(Plan(1, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}));
  s.publishPlan(Plan(2, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(3), ParticipantId(4)}));
  s.publishPlan(Plan(3, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(5), ParticipantId(6)}));
  s.publishPlan(Plan(4, 1, CollectiveKind::ALL_REDUCE, {ParticipantId(2), ParticipantId(3)}));
}

CollectiveRequestId Submit(Scheduler& s, std::uint64_t collective, std::uint64_t a, std::uint64_t b, std::uint64_t plan) {
  return s.submit(Req(collective, CollectiveKind::ALL_REDUCE, {ParticipantId(a), ParticipantId(b)}, G2(), plan, 1, OverlapMode::OVERLAP_ALLOWED));
}

// Schedules once and returns the single grant it issued (nullopt otherwise).
std::optional<CollectiveGrant> GrantOne(Scheduler& s) {
  auto dec = s.schedule();
  if (dec.grants.size() != 1) return std::nullopt;
  return s.grantById(dec.grants[0]);
}
}  // namespace

// Fencing a worker ends its active grant: the grant leaves the active count
// (which otherwise leaks until the bound stops all scheduling) and is revoked
// at the fabric.
TEST(regression_fence_ends_active_grant) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab, 1));
  Populate(s);
  Submit(s, 1, 1, 2, 1);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  CHECK(s.handoff(*g1));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
  s.fenceWorkerBoot(WorkerId(1), WorkerBootId(11));
  CHECK_EQ(s.info().active_grants, std::size_t{0});
  CHECK(fab->was_revoked(g1->id.value));
  Submit(s, 2, 3, 4, 2);
  auto dec = s.schedule();
  CHECK_EQ(dec.selected.size(), std::size_t{1});
  REQUIRE(dec.grants.size() == 1);
  auto g2 = s.grantById(dec.grants[0]); REQUIRE(g2.has_value());
  CHECK(s.handoff(*g2));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
}

// Superseding a request whose grant already ended must not take another
// grant's place in the active count.
TEST(regression_supersede_keeps_other_active_grant_counted) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab, 1));
  Populate(s);
  auto c1 = Submit(s, 1, 1, 2, 1);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  CHECK(s.handoff(*g1));
  CHECK(s.complete(*g1, WorkerBootId(11), SourceBootId(100)));
  Submit(s, 2, 3, 4, 2);
  auto g2 = GrantOne(s); REQUIRE(g2.has_value());
  CHECK(s.handoff(*g2));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
  auto r = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, G2(), 1, 1, OverlapMode::OVERLAP_ALLOWED);
  r.id = c1; r.gen = CollectiveRequestGeneration(2);
  s.supersede(std::move(r));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
  Submit(s, 3, 5, 6, 3);
  auto dec = s.schedule();
  CHECK(dec.selected.empty());  // the bound of 1 is held by C2
}

// Superseding a request revokes its in-flight grant at the fabric.
TEST(regression_supersede_revokes_in_flight_grant) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab));
  Populate(s);
  auto c1 = Submit(s, 1, 1, 2, 1);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  CHECK(s.handoff(*g1));
  auto r = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, G2(), 1, 1, OverlapMode::OVERLAP_ALLOWED);
  r.id = c1; r.gen = CollectiveRequestGeneration(2);
  s.supersede(std::move(r));
  CHECK(fab->was_revoked(g1->id.value));
  CHECK_EQ(s.info().active_grants, std::size_t{0});
  auto g2 = GrantOne(s); REQUIRE(g2.has_value());
  CHECK(s.handoff(*g2));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
}

// Grants issued by separate schedule() calls are not checked against each
// other; a grant conflicting with a dispatched one is not handed off until
// that one ends.
TEST(regression_conflicting_grants_not_dispatched_together) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab));
  Populate(s);
  Submit(s, 1, 1, 2, 1);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  Submit(s, 2, 2, 3, 4);  // shares participant 2 with C1
  auto g2 = GrantOne(s); REQUIRE(g2.has_value());
  CHECK(s.handoff(*g1));
  CHECK(!s.handoff(*g2));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
  CHECK_EQ(fab->accepted.size(), std::size_t{1});
  CHECK(s.complete(*g1, WorkerBootId(11), SourceBootId(100)));
  CHECK(s.handoff(*g2));  // C1 ended; C2's grant is still valid
  CHECK_EQ(s.info().active_grants, std::size_t{1});
}

// The active-grant bound also holds for grants issued by separate schedule()
// calls before any of them was dispatched.
TEST(regression_bound_holds_across_schedules) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab, 1));
  Populate(s);
  Submit(s, 1, 1, 2, 1);
  Submit(s, 2, 3, 4, 2);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  auto g2 = GrantOne(s); REQUIRE(g2.has_value());
  CHECK(s.handoff(*g1));
  CHECK(!s.handoff(*g2));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
}

// A grant the fabric refused gives up the slot and participants its dispatch
// took, and cannot complete.
TEST(regression_refused_grant_releases_dispatch) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab));
  Populate(s);
  Submit(s, 1, 1, 2, 1);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  fab->accept_result = false;
  CHECK(!s.handoff(*g1));
  CHECK(fab->was_revoked(g1->id.value));
  CHECK(!s.complete(*g1, WorkerBootId(11), SourceBootId(100)));
  CHECK_EQ(s.info().active_grants, std::size_t{0});
  fab->accept_result = true;
  Submit(s, 2, 2, 3, 4);
  auto g2 = GrantOne(s); REQUIRE(g2.has_value());
  CHECK(s.handoff(*g2));
}

// A grant superseded while the fabric decided is not activated, even after the
// request's new generation was granted and became active meanwhile.
TEST(regression_superseded_dispatch_not_activated) {
  auto fab = std::make_shared<ScriptedFabric>();
  Scheduler s(Opts(fab));
  Populate(s);
  auto c1 = Submit(s, 1, 1, 2, 1);
  auto g1 = GrantOne(s); REQUIRE(g1.has_value());
  bool second_active = false;
  fab->on_accept = [&](const CollectiveGrant&) {
    auto r = Req(1, CollectiveKind::ALL_REDUCE, {ParticipantId(1), ParticipantId(2)}, G2(), 1, 1, OverlapMode::OVERLAP_ALLOWED);
    r.id = c1; r.gen = CollectiveRequestGeneration(2);
    s.supersede(std::move(r));
    auto g2 = GrantOne(s);
    second_active = g2.has_value() && s.handoff(*g2);
  };
  CHECK(!s.handoff(*g1));
  CHECK(second_active);
  CHECK(fab->was_revoked(g1->id.value));
  CHECK_EQ(s.info().active_grants, std::size_t{1});
  CHECK(!s.complete(*g1, WorkerBootId(11), SourceBootId(100)));
}
