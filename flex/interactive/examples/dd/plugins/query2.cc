#include <queue>
#include <random>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "recom_reason.h"
namespace gs {

// Recommend alumni for the input user.
class Query2 : public AppBase {
 public:
  static constexpr int ACTIVE_DAYS_THRESHOLD = 20;
  Query2(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        subIndustry_label_id_(
            graph_.schema().get_vertex_label_id("SubIndustry")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        study_at_label_id_(graph_.schema().get_edge_label_id("StudyAt")),
        work_at_label_id_(graph_.schema().get_edge_label_id("WorkAt")),
        friend_label_id_(graph_.schema().get_edge_label_id("Friend")),
        involved_label_id_(graph_.schema().get_edge_label_id("Involved")),

        chat_in_group_label_id_(
            graph_.schema().get_edge_label_id("ChatInGroup")),
        user_subIndustry_col_(
            *(std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>(
                graph.get_vertex_property_column(user_label_id_,
                                                 "subIndustry")))),
        user_city_col_(
            *(std::dynamic_pointer_cast<gs::StringMapColumn<uint16_t>>(
                graph.get_vertex_property_column(user_label_id_, "city")))),
        user_roleName_col_(*(std::dynamic_pointer_cast<TypedColumn<uint8_t>>(
            graph.get_vertex_property_column(user_label_id_, "roleName")))),
        user_active_days_col_(*(std::dynamic_pointer_cast<TypedColumn<uint8_t>>(
            graph.get_vertex_property_column(user_label_id_, "activeDays")))),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        org_num_(graph.graph().vertex_num(ding_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        mt_(random_device_()) {}

  bool check_same_org(const GraphView<char_array<20>>& work_at,
                      const GraphView<char_array<16>>& study_at,
                      std::unordered_set<vid_t>& root_work_at,
                      std::unordered_set<vid_t>& root_study_at, vid_t v) {
    if (root_work_at.size()) {
      const auto& oe = work_at.get_edges(v);
      for (auto& e : oe) {
        if (root_work_at.count(e.neighbor)) {
          return true;
        }
      }
    }
    if (root_study_at.size()) {
      const auto& oe = study_at.get_edges(v);
      for (auto& e : oe) {
        if (root_study_at.count(e.neighbor)) {
          return true;
        }
      }
    }
    return false;
  }

  bool Query(Decoder& input, Encoder& output) {
    static constexpr int RETURN_LIMIT = 20;
    int64_t oid = input.get_long();
    gs::vid_t root;
    auto txn = graph_.GetReadTransaction();
    graph_.graph().get_lid(user_label_id_, oid, root);
    auto subIndustry = user_subIndustry_col_.get_view(root);
    auto subIndustryIdx = user_subIndustry_col_.get_idx(root);
    if (subIndustry == "") {
      return true;
    }
    const auto& study_at = txn.GetOutgoingGraphView<char_array<16>>(
        user_label_id_, ding_edu_org_label_id_, study_at_label_id_);
    const auto& work_at = txn.GetOutgoingGraphView<char_array<20>>(
        user_label_id_, ding_org_label_id_, work_at_label_id_);
    std::unordered_set<vid_t> root_work_at, root_study_at;
    {
      const auto& work_oe = work_at.get_edges(root);
      for (auto& e : work_oe) {
        root_work_at.emplace(e.neighbor);
      }
      const auto& study_oe = study_at.get_edges(root);
      for (auto& e : study_oe) {
        root_study_at.emplace(e.neighbor);
      }
    }
    const auto& intimacy_edges = txn.GetOutgoingImmutableEdges<char_array<4>>(
        user_label_id_, root, user_label_id_, intimacy_label_id_);
    std::vector<vid_t> intimacy_users;

    for (auto& e : intimacy_edges) {
      if (!check_same_org(work_at, study_at, root_work_at, root_study_at,
                          e.neighbor))
        intimacy_users.emplace_back(e.get_neighbor());
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
    {
      std::sort(intimacy_users.begin(), intimacy_users.end());
      auto len = std::unique(intimacy_users.begin(), intimacy_users.end()) -
                 intimacy_users.begin();
      intimacy_users.resize(len);
      int j = 0;
      int k = 0;
      for (auto i = 0; i < intimacy_users.size(); ++i) {
        if (intimacy_users[i] == root)
          continue;
        while (j < friends.size() && friends[j] < intimacy_users[i])
          ++j;
        if (j < friends.size() && friends[j] == intimacy_users[i]) {
        } else {
          intimacy_users[k++] = intimacy_users[i];
        }
      }
      intimacy_users.resize(k);
    }

    std::vector<vid_t> intimacy_users_tmp;
    // root -> Intimacy -> Users
    for (auto& v : intimacy_users) {
      if (subIndustryIdx == user_subIndustry_col_.get_idx(v) &&
          user_active_days_col_.get_view(v) > ACTIVE_DAYS_THRESHOLD) {
        intimacy_users_tmp.emplace_back(v);
        vis_set.emplace(v);
      }
    }
    std::vector<vid_t> return_vec;
    std::vector<RecomReason> return_reasons;
    if (intimacy_users_tmp.size() <= RETURN_LIMIT / 5) {
      //@TODO 推荐理由
      for (auto user : intimacy_users_tmp) {
        return_vec.emplace_back(user);
        return_reasons.emplace_back(RecomReason::Active());
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
        return_reasons.emplace_back(RecomReason::Active());
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
    // friends num + groups num,vid_t
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
              subIndustryIdx == user_subIndustry_col_.get_idx(e.neighbor)) {
            if (common_group_users[e.neighbor].size() < 2) {
              // common_users_vec.emplace_back(e.neighbor);
              common_group_users[e.neighbor].emplace_back(g);
            }
          }
        }
      }
    }
    for (auto& pair : common_group_users) {
      common_users_vec.emplace_back(pair.first);
      auto& value = pair.second;
      if (value.size() == 1) {
        common_users_reason_tmp.emplace_back(true, value[0], INVALID_VID);
      } else if (value.size() >= 2) {
        common_users_reason_tmp.emplace_back(true, value[0], value[1]);
      }
    }

    std::unordered_map<vid_t, std::vector<vid_t>> common_friend_users;
    // 2-hop friends
    if (friends.size()) {
      for (size_t i = 0; i < friends.size(); ++i) {
        auto cur = friends[i];
        const auto& ie = friends_ie.get_edges(cur);
        const auto& oe = friends_oe.get_edges(cur);
        for (auto& e : ie) {
          if (!vis_set.count(e.neighbor) &&
              subIndustryIdx == user_subIndustry_col_.get_idx(e.neighbor)) {
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
              subIndustryIdx == user_subIndustry_col_.get_idx(e.neighbor)) {
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

    for (auto& pair : common_friend_users) {
      common_users_vec.emplace_back(pair.first);
      auto& value = pair.second;
      if (value.size() == 1) {
        common_users_reason_tmp.emplace_back(false, value[0], INVALID_VID);
      } else if (value.size() >= 2) {
        common_users_reason_tmp.emplace_back(false, value[0], value[1]);
      }
    }

    int res = RETURN_LIMIT * 3 / 5;
    std::shuffle(common_users_vec.begin(), common_users_vec.end(), mt_);
    int idx = common_users_vec.size();
    while (return_vec.size() < res && idx > 0) {
      auto vid = common_users_vec[idx - 1];
      auto& value = common_users_reason_tmp[idx - 1];

      if (!vis_set.count(vid)) {
        vis_set.emplace(vid);
        if (!check_same_org(work_at, study_at, root_work_at, root_study_at,
                            vid)) {
          return_vec.emplace_back(vid);
          if (std::get<0>(value)) {
            return_reasons.emplace_back(RecomReason::CommonGroup(
                std::get<1>(value), std::get<2>(value)));
          } else {
            return_reasons.emplace_back(RecomReason::CommonFriend(
                std::get<1>(value), std::get<2>(value)));
          }
        }
        --idx;
      }
    }

    int loop_limit = 300000;
    res = RETURN_LIMIT - return_vec.size();
    const auto& involved_ie = txn.GetIncomingImmutableEdges<grape::EmptyType>(
        subIndustry_label_id_, subIndustryIdx, user_label_id_,
        involved_label_id_);
    auto root_city = user_city_col_.get_view(root);
    bool exist_city = true;
    if (root_city == " ") {
      exist_city = false;
    }
    auto root_city_id = user_city_col_.get_idx(root);
    auto root_roleName_id = user_roleName_col_.get_view(root);

    std::vector<uint32_t> tmp;
    std::vector<vid_t> cand;
    size_t idx1 = 0, idx2 = 0;
    if (involved_ie.estimated_degree() > 1) {
      for (auto& e : involved_ie) {
        loop_limit--;
        if (loop_limit == 0) {
          break;
        }
        if (!vis_set.count(e.neighbor) &&
            !check_same_org(work_at, study_at, root_work_at, root_study_at,
                            e.neighbor)) {
          auto city_id = user_city_col_.get_idx(e.neighbor);
          auto roleName_id = user_roleName_col_.get_view(e.neighbor);
          if ((!exist_city || city_id != root_city_id) &&
              roleName_id != root_roleName_id) {
            if (tmp.size() < res) {
              tmp.emplace_back(e.neighbor);
            } else {
              std::uniform_int_distribution<int> dist(0, idx1);
              auto ind = dist(mt_);
              if (ind < res) {
                tmp[ind] = e.neighbor;
              }
            }
            idx1++;
          } else {
            if (cand.size() < res) {
              cand.emplace_back(e.neighbor);
            } else {
              std::uniform_int_distribution<int> dist(0, idx2);
              auto ind = dist(mt_);
              if (ind < res) {
                cand[ind] = e.neighbor;
              }
            }
            idx2++;
          }
        }
      }
    }
    for (auto v : cand) {
      return_vec.emplace_back(v);
      return_reasons.emplace_back(RecomReason::Default());
    }
    for (int i = 0; i < tmp.size() && return_vec.size() < RETURN_LIMIT; i++) {
      return_vec.emplace_back(tmp[i]);
      return_reasons.emplace_back(RecomReason::Default());
    }
    CHECK(return_vec.size() <= RETURN_LIMIT);
    serialize_result(graph_, output, user_label_id_, return_vec,
                     return_reasons);

    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t user_label_id_;
  label_t ding_org_label_id_;
  label_t ding_edu_org_label_id_;
  label_t ding_group_label_id_;
  label_t subIndustry_label_id_;
  label_t workat_label_id_;

  label_t friend_label_id_;
  label_t chat_in_group_label_id_;
  label_t intimacy_label_id_;
  label_t work_at_label_id_;
  label_t study_at_label_id_;
  label_t involved_label_id_;

  gs::StringMapColumn<uint16_t>& user_subIndustry_col_;
  gs::StringMapColumn<uint16_t>& user_city_col_;
  gs::TypedColumn<uint8_t>& user_active_days_col_;
  gs::TypedColumn<uint8_t>& user_roleName_col_;
  size_t edu_org_num_ = 0;
  size_t users_num_ = 0;
  size_t org_num_ = 0;
  std::random_device random_device_;
  std::mt19937 mt_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query2* app = new gs::Query2(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query2* casted = static_cast<gs::Query2*>(app);
  delete casted;
}
}