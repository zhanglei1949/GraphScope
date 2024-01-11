#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

static constexpr int32_t EMPLOYEE_CNT = 100000;

enum RecomReasonType {
  kClassMateAndColleague = 0,  // 同学兼同事
  kExColleague = 1,            // 前同事
  kCommonFriend = 2,           // 共同好友
  kCommonGroup = 3,            // 共同群
  kCommunication = 4,          // 最近沟通
  kActiveUser = 5,             // 活跃用户
  kDefault = 6                 // 默认
};

struct RecomReason {
  RecomReason() : type(kDefault) {}
  RecomReasonType type;
  int32_t num_common_group_or_friend;
  std::vector<vid_t> common_group_or_friend;
  int32_t org_id;  // 同事或者同学的组织id
};

// Recommend alumni for the input user.
class Query0 : public AppBase {
 public:
  Query0(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        workat_label_id_(graph_.schema().get_edge_label_id("WorkAt")),
        friend_label_id_(graph_.schema().get_edge_label_id("Friend")),
        chat_in_group_label_id_(
            graph_.schema().get_edge_label_id("ChatInGroup")),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        org_num_(graph.graph().vertex_num(ding_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        is_user_in_org_inited_(false),
        is_user_friend_inited_(false) {}

  bool Query(Decoder& input, Encoder& output) {
    int64_t oid = input.get_long();
    gs::vid_t root;
    auto txn = graph_.GetReadTransaction();
    graph_.graph().get_lid(user_label_id_, oid, root);
    LOG(INFO) << "PASS\n";
    const auto& workat_edges = txn.GetOutgoingEdges<char_array<20>>(
        user_label_id_, root, ding_org_label_id_, workat_label_id_);
    std::unordered_set<vid_t> orgs;
    for (auto& e : workat_edges) {
      orgs.emplace(e.get_neighbor());  // org
    }
    LOG(INFO) << "PASS\n";
    const auto& intimacy_edges = txn.GetOutgoingImmutableEdges<char_array<4>>(
        user_label_id_, root, user_label_id_, intimacy_label_id_);
    std::vector<vid_t> intimacy_users;
    for (auto& e : intimacy_edges) {
      intimacy_users.emplace_back(e.get_neighbor());
    }
    std::sort(intimacy_users.begin(), intimacy_users.end());
    const auto& friend_edges_oe =
        txn.GetOutgoingImmutableEdges<grape::EmptyType>(
            user_label_id_, root, user_label_id_, friend_label_id_);
    const auto& friend_edges_ie =
        txn.GetIncomingImmutableEdges<grape::EmptyType>(
            user_label_id_, root, user_label_id_, friend_label_id_);
    std::vector<vid_t> friends;
    std::unordered_set<vid_t> vis_set, friend_set;
    for (auto& e : friend_edges_oe) {
      friends.emplace_back(e.get_neighbor());
      vis_set.emplace(e.get_neighbor());
      friend_set.emplace(e.get_neighbor());
    }
    for (auto& e : friend_edges_ie) {
      friends.emplace_back(e.get_neighbor());
      vis_set.emplace(e.get_neighbor());
      friend_set.emplace(e.get_neighbor());
    }
    LOG(INFO) << "PASS\n";
    std::sort(friends.begin(), friends.end());
    {
      auto len = std::unique(intimacy_users.begin(), intimacy_users.end()) -
                 intimacy_users.begin();
      LOG(INFO) << len << "\n";
      intimacy_users.resize(len);
      int j = 0;
      int k = 0;
      for (auto i = 0; i < intimacy_users.size(); ++i) {
        while (j < friends.size() && friends[j] < intimacy_users[i])
          ++j;
        if (j < friends.size() && friends[j] == intimacy_users[i]) {
        } else {
          intimacy_users[k++] = intimacy_users[i];
        }
      }
      intimacy_users.resize(k);
    }
    LOG(INFO) << "PASS\n";

    auto workat_oe = txn.GetOutgoingGraphView<char_array<20>>(
        user_label_id_, ding_org_label_id_, workat_label_id_);

    std::vector<vid_t> ans;
    for (auto& v : intimacy_users) {
      const auto& oe = workat_oe.get_edges(v);
      for (auto& e : oe) {
        if (orgs.count(e.get_neighbor())) {
          ans.emplace_back(v);
          if (ans.size() > 20) {
            return true;
          }
          break;
        }
      }
    }
    auto workat_ie = txn.GetIncomingGraphView<char_array<20>>(
        ding_org_label_id_, user_label_id_, workat_label_id_);
    auto friends_ie = txn.GetIncomingImmutableGraphView<grape::EmptyType>(
        user_label_id_, user_label_id_, friend_label_id_);
    auto friends_oe = txn.GetOutgoingImmutableGraphView<grape::EmptyType>(
        user_label_id_, user_label_id_, friend_label_id_);

    std::unordered_set<vid_t> groups;
    auto group_oes = txn.GetOutgoingImmutableGraphView<grape::EmptyType>(
        user_label_id_, ding_group_label_id_, chat_in_group_label_id_);
    {
      const auto& oe = group_oes.get_edges(root);
      for (auto& e : oe) {
        groups.emplace(e.get_neighbor());
      }
    }
    std::vector<std::pair<int, vid_t>> users;

    for (auto o : orgs) {
      const auto& ie = workat_ie.get_edges(o);
      for (auto& e : ie) {
        auto nbr = e.get_neighbor();
        if (vis_set.count(nbr)) {
          continue;
        }
        auto ies = friends_ie.get_edges(nbr);
        int num = 0;
        for (auto& e : ies) {
          if (friend_set.count(e.get_neighbor())) {
            ++num;
          }
        }
        auto oes = friends_oe.get_edges(nbr);
        for (auto& e : oes) {
          if (friend_set.count(e.get_neighbor())) {
            ++num;
          }
        }
        auto group_oe = group_oes.get_edges(nbr);
        for (auto& e : group_oe) {
          if (groups.count(e.get_neighbor())) {
            ++num;
          }
        }
        users.emplace_back(num, nbr);
      }
    }
    std::sort(users.begin(), users.end());
    size_t idx = users.size();
    while (ans.size() < 50 && idx > 0) {
      ans.emplace_back(users[idx - 1].second);
      --idx;
    }
    std::vector<int64_t> vec;
    for (auto vid : ans) {
      vec.emplace_back(graph_.graph().get_oid(user_label_id_, vid).AsInt64());
    }
    for (auto oid : vec) {
      output.put_long(oid);
    }

    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t user_label_id_;
  label_t ding_org_label_id_;
  label_t ding_edu_org_label_id_;
  label_t ding_group_label_id_;
  label_t workat_label_id_;

  label_t friend_label_id_;
  label_t chat_in_group_label_id_;
  label_t intimacy_label_id_;
  label_t work_at_label_id_;
  label_t study_at_label_id_;

  size_t edu_org_num_ = 0;
  size_t users_num_ = 0;
  size_t org_num_ = 0;

  bool is_user_in_org_inited_ = false;
  bool is_user_friend_inited_ = false;

  // A hash_set to store the users that are in the same org as the input user
  // Can be reused for different stage of recommendation.
  std::unordered_set<vid_t> users_in_org_ids_;
  std::unordered_set<vid_t> users_are_friends_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query0* app = new gs::Query0(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}