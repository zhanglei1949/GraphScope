#include <queue>
#include <random>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "recom_reason.h"
namespace gs {

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
        mt_(random_device_()) {}

  bool check_same_org(const std::unordered_set<uint32_t>& st,
                      const GraphView<char_array<20>>& view, uint32_t root,
                      std::unordered_map<uint32_t, bool>& mem) {
    if (mem.count(root))
      return mem[root];

    const auto& ie = view.get_edges(root);
    mem[root] = false;
    for (auto& e : ie) {
      if (st.count(e.neighbor)) {
        mem[root] = true;
        break;
      }
    }
    return mem[root];
  }
  bool Query(Decoder& input, Encoder& output) {
    static constexpr int RETURN_LIMIT = 20;
    int64_t oid = input.get_long();
    LOG(INFO) << "Query0: " << oid;
    gs::vid_t root;
    auto txn = graph_.GetReadTransaction();
    graph_.graph().get_lid(user_label_id_, oid, root);
    const auto& workat_edges = txn.GetOutgoingEdges<char_array<20>>(
        user_label_id_, root, ding_org_label_id_, workat_label_id_);
    const auto& workat_ie = txn.GetIncomingGraphView<char_array<20>>(
        ding_org_label_id_, user_label_id_, workat_label_id_);
    std::unordered_set<vid_t> orgs;
    size_t sum = 0;
    for (auto& e : workat_edges) {
      int d = workat_ie.get_edges(e.neighbor).estimated_degree();
      if (d <= 1) {
        continue;
      }
      orgs.emplace(e.get_neighbor());  // org
      sum += d;
    }
    if (sum == 0) {
      output.put_int(0);
      return true;
    }

    const auto& intimacy_edges = txn.GetOutgoingImmutableEdges<char_array<4>>(
        user_label_id_, root, user_label_id_, intimacy_label_id_);
    std::vector<vid_t> intimacy_users;

    for (auto& e : intimacy_edges) {
      intimacy_users.emplace_back(e.get_neighbor());
    }
    std::sort(intimacy_users.begin(), intimacy_users.end());
    {
      auto len = std::unique(intimacy_users.begin(), intimacy_users.end()) -
                 intimacy_users.begin();
      intimacy_users.resize(len);
    }

    const auto& friend_edges_oe =
        txn.GetOutgoingImmutableEdges<grape::EmptyType>(
            user_label_id_, root, user_label_id_, friend_label_id_);
    const auto& friend_edges_ie =
        txn.GetIncomingImmutableEdges<grape::EmptyType>(
            user_label_id_, root, user_label_id_, friend_label_id_);
    std::vector<vid_t> friends;
    std::unordered_set<vid_t> vis_set;
    vis_set.emplace(root);
    get_friends(friends, vis_set, friend_edges_oe, friend_edges_ie);

    auto workat_oe = txn.GetOutgoingGraphView<char_array<20>>(
        user_label_id_, ding_org_label_id_, workat_label_id_);
    std::unordered_map<uint32_t, bool> mem;
    std::vector<vid_t> intimacy_users_tmp;
    // root -> Intimacy -> Users
    for (auto& v : intimacy_users) {
      if (!vis_set.count(v) && check_same_org(orgs, workat_oe, v, mem)) {
        intimacy_users_tmp.emplace_back(v);
      }
    }
    std::vector<vid_t> return_vec;
    std::vector<RecomReason> returns_reasons;
    if (intimacy_users_tmp.size() <= RETURN_LIMIT / 5) {
      //@TODO 推荐理由
      for (auto user : intimacy_users_tmp) {
        return_vec.emplace_back(user);
        returns_reasons.emplace_back(RecomReason::Default());
        vis_set.emplace(user);
      }
    } else {
      // random pick
      std::uniform_int_distribution<int> dist(0, intimacy_users_tmp.size() - 1);
      std::unordered_set<int> st;
      for (int i = 0; i < RETURN_LIMIT / 5; ++i) {
        int ind = dist(mt_);
        while (st.count(ind)) {
          ++ind;
          ind %= intimacy_users_tmp.size();
        }
        st.emplace(ind);
        return_vec.emplace_back(intimacy_users_tmp[ind]);
        returns_reasons.emplace_back(RecomReason::Default());
        vis_set.emplace(intimacy_users_tmp[ind]);
      }
    }

    auto friends_ie = txn.GetIncomingImmutableGraphView<grape::EmptyType>(
        user_label_id_, user_label_id_, friend_label_id_);
    auto friends_oe = txn.GetOutgoingImmutableGraphView<grape::EmptyType>(
        user_label_id_, user_label_id_, friend_label_id_);

    std::unordered_set<vid_t> groups;
    // root -> oe chatInGroup -> groups
    auto group_oes = txn.GetOutgoingImmutableGraphView<grape::EmptyType>(
        user_label_id_, ding_group_label_id_, chat_in_group_label_id_);
    {
      const auto& oe = group_oes.get_edges(root);
      for (auto& e : oe) {
        groups.emplace(e.get_neighbor());
      }
    }
    std::unordered_map<vid_t, std::vector<vid_t>> common_group_users;
    std::vector<vid_t> common_users_vec;
    std::vector<std::tuple<bool, vid_t, vid_t>> common_users_reason_tmp;
    {
      // groups -> ie chatInGroup -> users
      auto group_ies = txn.GetIncomingImmutableGraphView<grape::EmptyType>(
          ding_group_label_id_, user_label_id_, chat_in_group_label_id_);
      for (auto g : groups) {
        auto d = group_ies.get_edges(g).estimated_degree();
        if (d <= 1)
          continue;
        auto ie = group_ies.get_edges(g);
        for (auto e : ie) {
          if (!vis_set.count(e.neighbor) &&
              check_same_org(orgs, workat_oe, e.neighbor, mem)) {
            if (common_group_users[e.neighbor].size() < 2) {
              common_group_users[e.neighbor].emplace_back(g);
            }
            // common_group_users[e.neighbor] += 1;
            // if (common_group_users[e.neighbor] == 1) {
            //   common_users_vec.emplace_back(e.neighbor);
            // }
          }
        }
      }
    }

    for (auto& pair : common_group_users) {
      auto& value = pair.second;
      if (value.size() == 1) {
        common_users_vec.emplace_back(pair.first);
        common_users_reason_tmp.emplace_back(true, value[0], INVALID_VID);
      } else if (value.size() >= 2) {
        common_users_vec.emplace_back(pair.first);
        common_users_reason_tmp.emplace_back(true, value[0], value[1]);
      }
    }

    LOG(INFO) << "common group user count:" << common_users_vec.size(); 

    std::unordered_map<vid_t, std::vector<vid_t>> common_friend_users;
    // 2-hop friends
    if (friends.size()) {
      for (size_t i = 0; i < friends.size(); ++i) {
        auto cur = friends[i];
        const auto& ie = friends_ie.get_edges(cur);
        const auto& oe = friends_oe.get_edges(cur);
        for (auto& e : ie) {
          if (!vis_set.count(e.neighbor) &&
              check_same_org(orgs, workat_oe, e.neighbor, mem)) {
            // common_friend_users[e.neighbor] += 1;
            if (common_friend_users[e.neighbor].size() < 2 &&
                common_group_users.count(e.neighbor) == 0) {
              // common_users_vec.emplace_back(e.neighbor);
              common_friend_users[e.neighbor].emplace_back(cur);
            }
          }
        }
        for (auto& e : oe) {
          if (!vis_set.count(e.neighbor) &&
              check_same_org(orgs, workat_oe, e.neighbor, mem)) {
            // common_friend_users[e.neighbor] += 1;
            if (common_friend_users[e.neighbor].size() < 2 &&
                common_group_users.count(e.neighbor) == 0) {
              // common_users_vec.emplace_back(e.neighbor);
              common_friend_users[e.neighbor].emplace_back(cur);
            }
          }
        }
      }
    }
    LOG(INFO) << "common friend user count:" << common_friend_users.size(); 

    for (auto& pair : common_friend_users) {
      auto& value = pair.second;
      if (value.size() == 1) {
        common_users_vec.emplace_back(pair.first);
        common_users_reason_tmp.emplace_back(false, value[0], INVALID_VID);
      } else if (value.size() >= 2) {
        common_users_vec.emplace_back(pair.first);
        common_users_reason_tmp.emplace_back(false, value[0], value[1]);
      }
    }

    int res = RETURN_LIMIT - return_vec.size();

    LOG(INFO) << "select: " << res;
    // less than 20 users
    CHECK(common_users_vec.size() == common_users_reason_tmp.size());
    if (common_users_vec.size() <= res) {
      std::shuffle(common_users_vec.begin(), common_users_vec.end(), mt_);
      for (auto i = 0; i < common_users_vec.size(); ++i) {
        return_vec.emplace_back(common_users_vec[i]);
        auto& value = common_users_reason_tmp[i];
        if (std::get<0>(value)) {
          returns_reasons.emplace_back(
              RecomReason::CommonGroup(std::get<1>(value), std::get<2>(value)));
        } else {
          returns_reasons.emplace_back(RecomReason::CommonFriend(
              std::get<1>(value), std::get<2>(value)));
        }
      }
    } else {
      std::uniform_int_distribution<int> dist(0, common_users_vec.size() - 1);
      std::unordered_set<int> st;
      for (int i = 0; i < res; ++i) {
        int ind = dist(mt_);
        while (st.count(ind)) {
          ind++;
          ind %= common_users_vec.size();
        }
        st.emplace(ind);
        return_vec.emplace_back(common_users_vec[ind]);
        auto& value = common_users_reason_tmp[ind];
        if (std::get<0>(value)) {
          returns_reasons.emplace_back(
              RecomReason::CommonGroup(std::get<1>(value), std::get<2>(value)));
        } else {
          returns_reasons.emplace_back(RecomReason::CommonFriend(
              std::get<1>(value), std::get<2>(value)));
        }
      }
    }
    LOG(INFO) << "before output";
    serialize_result(graph_, output, user_label_id_, return_vec,
                     returns_reasons);
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
  label_t study_at_label_id_;

  size_t edu_org_num_ = 0;
  size_t users_num_ = 0;
  size_t org_num_ = 0;
  std::random_device random_device_;
  std::mt19937 mt_;
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
