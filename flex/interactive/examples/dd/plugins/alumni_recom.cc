
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
class AlumniRecom : public AppBase {
 public:
  using intimacy_edge_type = char_array<4>;
  using workAt_edge_type = char_array<20>;
  using studyAt_edge_type = char_array<16>;
  using friend_edge_type = grape::EmptyType;
  AlumniRecom(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        friend_label_id_(graph.schema().get_edge_label_id("Friend")),
        work_at_label_id_(graph.schema().get_edge_label_id("WorkAt")),
        study_at_label_id_(graph.schema().get_edge_label_id("StudyAt")),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),

        is_user_in_org_inited_(false),
        is_user_friend_inited_(false) {}

  bool Query(Decoder& input, Encoder& output) {
    is_user_in_org_inited_ = false;
    is_user_friend_inited_ = false;
    users_are_friends_.clear();
    users_studyAt_edu_org_.clear();
    valid_edu_org_ids_.clear();

    int64_t user_id = input.get_long();
    int32_t page_id = input.get_int();
    int32_t limit = input.get_int();
    int32_t start_ind = (page_id) *limit;
    int32_t end_ind = (page_id + 1) * limit;
    auto txn = graph_.GetReadTransaction();

    auto user_study_at_edu_org_oe_view_ =
        txn.GetOutgoingGraphView<studyAt_edge_type>(
            user_label_id_, ding_edu_org_label_id_, study_at_label_id_);
    auto user_user_friend_oe_view_ = txn.GetOutgoingGraphView<friend_edge_type>(
        user_label_id_, user_label_id_, friend_label_id_);
    auto user_user_friend_ie_view_ = txn.GetIncomingGraphView<friend_edge_type>(
        user_label_id_, user_label_id_, friend_label_id_);
    auto user_user_intimacy_view =
        txn.GetOutgoingImmutableGraphView<intimacy_edge_type>(
            user_label_id_, user_label_id_, intimacy_label_id_);

    vid_t vid;
    if (!txn.GetVertexIndex(user_label_id_, Any::From(user_id), vid)) {
      LOG(ERROR) << "Vertex not found: " << user_id;
      return false;
    }

    // Init basic ds.
    init_user_friend_set(vid, user_user_friend_oe_view_,
                         user_user_friend_ie_view_);
    init_user_studyAt_edu_org(vid, user_study_at_edu_org_oe_view_);

    LOG(INFO) << "Valid Edu org ids size: " << valid_edu_org_ids_.size();
    // sort edu_org_ids asc
    std::sort(valid_edu_org_ids_.begin(), valid_edu_org_ids_.end());
    init_user_friend(vid, user_user_friend_view_oe, user_user_friend_view_ie);

    std::vector<std::pair<vid_t, uint16_t>> intimacy_users;
    //首先通过亲密度边，拿到所有有亲密度的User
    // only keep the user that are in the same org, AND not friend
    traverse_intimacy_view(vid, user_user_intimacy_view, user_edu_org_view,
                           intimacy_users);

    VLOG(10) << "intimacy_users size: " << intimacy_users.size();
    if (start_ind > intimacy_users.size()) {
      // intimacy_users.size () < start_ind < end_ind
      start_ind = start_ind - intimacy_users.size();
      end_ind = end_ind - intimacy_users.size();
      std::vector<std::pair<vid_t, RecomReason>> res =
          try_without_intimacy(txn, vid, start_ind, end_ind);
      serialize(res, output, limit);
      txn.Commit();
      return true;
    } else if (end_ind > intimacy_users.size()) {
      // In this case, the first part of recommend users are in initmacy users,
      // the second part are from try_with_intimacy
      // start_ind <= intimacy_users.size() < end_ind
      sort_intimacy_users(intimacy_users);
      std::vector<std::pair<vid_t, RecomReason>> second_part =
          try_without_intimacy(txn, vid, 0, end_ind - intimacy_users.size());
      // get the users from the first part, from start_ind to the end
      std::vector<std::pair<vid_t, uint16_t>> first_part;
      for (int32_t i = start_ind; i < intimacy_users.size(); ++i) {
        first_part.emplace_back(intimacy_users[i]);
      }
      serialize(first_part, second_part, output, limit);
      txn.Commit();
      return true;
    } else {
      sort_intimacy_users(intimacy_users);
      std::vector<std::pair<vid_t, uint16_t>> first_part;
      for (int32_t i = start_ind; i < end_ind; ++i) {
        first_part.emplace_back(intimacy_users[i]);
      }
      serialize(first_part, output, limit);
      txn.Commit();
      return true;
    }
  }

  // Get the users via (User)-[Friend]->(CommonFiend)-[Friend]->(TargetUsers)
  // Filter the users that are in the same org as the input user, and the users
  // that are already friends.
  std::vector<std::pair<vid_t, RecomReason>> try_get_common_friend_users(
      ReadTransaction& txn, vid_t vid) const {
    // 1. First get the friends of the input user
    CHECK(is_user_friend_inited_);
    // 2. Iterate over all friends, and get their friends
    std::unordered_map<vid_t, int> common_friends;
    auto user_user_friend_view_oe = txn.GetOutgoingGraphView<friend_edge_type>(
        user_label_id_, user_label_id_, friend_label_id_);
    auto user_user_friend_view_ie = txn.GetIncomingGraphView<friend_edge_type>(
        user_label_id_, user_label_id_, friend_label_id_);
    for (auto user_vid : users_are_friends_) {
      for (auto& edge : user_user_friend_view_oe.get_edges(user_vid)) {
        auto dst = edge.get_neighbor();
        // check is in the same org
        if (user_in_valid_org(dst) && !is_friend(dst)) {
          if (common_friends.count(dst) == 0) {
            common_friends[dst] = 1;
          } else {
            common_friends[dst] += 1;
          }
        }
      }
      for (auto& edge : user_user_friend_view_ie.get_edges(user_vid)) {
        auto dst = edge.get_neighbor();
        if (user_in_valid_org(dst) && !is_friend(dst)) {
          if (common_friends.count(dst) == 0) {
            common_friends[dst] = 1;
          } else {
            common_friends[dst] += 1;
          }
        }
      }
    }
    LOG(INFO) << "common friend size: " << common_friends.size();
  }

  auto get_potential_friends_via_joindate(
      ReadTransaction& txn, vid_t root, int32_t limit,
      const std::unordered_set<vid_t>& org_ids,
      std::unordered_set<vid_t>& visited, std::vector<vid_t>& res) {
    const auto& workat_ie = txn.GetIncomingGraphView<char_array<16>>(
        ding_org_label_id_, user_label_id_, study_at_label_id_);
    size_t sum = 0;
    for (auto org_id : org_ids) {
      auto ie = workat_ie.get_edges(org_id);
      sum += ie.estimated_degree();
    }
    if (sum == 0) {
      return;
    }
    // try to sample from each org, if we sample 5 results are all in visited,
    // then we stop.
    std::vector<vid_t> res;
    int32_t cnt = 0;
    int32_t sample_cont_failed_cnt = 0;  // sample continue failed count
    std::vector<vid_t> org_ids_vec(org_ids.begin(), org_ids.end());
    while (cnt < limit && sample_cont_failed_cnt < 5) {
      auto org_id = org_ids_vec[rand() % org_ids.size()];
      auto ie = workat_ie.get_edges(org_id);
      auto len = ie.estimated_degree();
      if (len == 0) {
        sample_cont_failed_cnt = 0;
        continue;
      }
      auto idx = rand() % len;
      auto nbr_slice = ie.slice();
      auto nbr = nbr_slice.get_index(idx);
      if (visited.count(nbr->neighbor)) {
        ++sample_cont_failed_cnt;
        continue;
      }
      // If not visied but the joinDate are not +/-3 years, we still skip it.
      auto edata = nbr->get_data();
      auto cur_ptr = static_cast<const char*>(edata.ptr);
      auto joinDate = *reinterpret_cast<const int64_t*>(cur_ptr);
      if (get_year_diff(joinDate, ) > 3) {
        ++sample_cont_failed_cnt;
        continue;
      }

      visited.emplace(vid);
      res.emplace_back(vid);
      ++cnt;
    }
  }

  auto get_potential_friends(ReadTransaction& txn, vid_t root, int32_t limit,
                             const std::unordered_set<vid_t>& org_ids,
                             std::unordered_set<vid_t>& visited) {
    // 1. Same major, skiped since currently empty.
    // 2. for Org joinDate +/-3 years
    std::vector<vid_t> res;
    get_potential_friends_via_joindate(txn, root, limit, org_ids, visited, res);
    // 3. Same city
    get_potential_friends_via_city(txn, root, limit, visited, res);
    // 4. Same profession
    get_potential_friends_via_profession(txn, root, limit, visited, res);
    // res.size() must be <= limit
    return res;
  }

  // 通过共同好友和共同群，拿到User,这里只保留和当前组织有过关系的用户
  void traverse_intimacy_view(
      vid_t vid, const GraphView<intimacy_edge_type>& user_user_intimacy_view,
      std::vector<std::pair<vid_t, uint16_t>>& intimacy_users) {
    if (org_employee_cnt >= EMPLOYEE_CNT) {
      LOG(INFO) << "traverse intimacy view: org_employee_cnt: "
                << org_employee_cnt << " >= " << EMPLOYEE_CNT;
      for (auto& edge : user_user_intimacy_view.get_edges(vid)) {
        auto dst = edge.get_neighbor();
        // only keep the user that are in the same org
        if (user_studied_at(dst, user_edu_org_view) && !is_friend(dst)) {
          auto fc = edge.get_data();
          auto cur_ptr = static_cast<const char*>(fc.ptr);
          // the first it a uint8_t, the second is a uint16_t
          auto intimacy = *reinterpret_cast<const uint8_t*>(cur_ptr);
          auto comm_score = *reinterpret_cast<const uint16_t*>(cur_ptr + 1);
          intimacy_users.emplace_back(dst, intimacy + comm_score);
        }
      }
    } else {
      LOG(INFO) << "traverse intimacy view: org_employee_cnt: "
                << org_employee_cnt << " < " << EMPLOYEE_CNT;

      for (auto& edge : user_user_intimacy_view.get_edges(vid)) {
        auto dst = edge.get_neighbor();
        if (users_studyAt_edu_org_.count(dst) > 0 && !is_friend(dst)) {
          auto fc = edge.get_data();
          auto cur_ptr = static_cast<const char*>(fc.ptr);
          // the first it a uint8_t, the second is a uint16_t
          auto intimacy = *reinterpret_cast<const uint8_t*>(cur_ptr);
          auto comm_score = *reinterpret_cast<const uint16_t*>(cur_ptr + 1);
          intimacy_users.emplace_back(dst, intimacy + comm_score);
        }
      }
    }
  }

  void init_user_studyAt_edu_org(
      vid_t user_vid,
      const GraphView<studyAt_edge_type>& user_study_at_edu_org_oe_view_,
      const GraphView<studyAt_edge_type>& user_study_at_edu_org_ie_view_) {
    CHECK(!is_user_in_org_inited_);
    CHECK(valid_edu_org_ids_.empty());
    {
      for (auto& edge : user_study_at_edu_org_oe_view_.get_edges(user_vid)) {
        auto dst = edge.get_neighbor();
        valid_edu_org_ids_.emplace_back(dst);
      }
    }
    // 如果所有edu_org的邻边加起来超过阈值，我们将通过用户来访问edu_org.
    // 如果所有edu_org的邻边加起来不超过阈值，我们将通过edu_org来访问用户.
    size_t org_employee_cnt = 0;
    {
      for (auto org_id : valid_edu_org_ids_) {
        org_employee_cnt +=
            user_study_at_edu_org_ie_view_.get_edges(org_id).estimated_degree();
      }
    }
    LOG(INFO) << "org_employee_cnt: " << org_employee_cnt;
    if (org_employee_cnt < EMPLOYEE_CNT) {
      for (auto org_id : valid_edu_org_ids_) {
        for (auto& edge : user_study_at_edu_org_oe_view_.get_edges(org_id)) {
          auto dst = edge.get_neighbor();
          users_studyAt_edu_org_.insert(dst);
        }
      }
    }
    is_user_in_org_inited_ = true;
  }

  // Suppose edu_org_ids are sorted asc.
  inline bool user_studied_at(
      vid_t user_vid, const std::vector<vid_t>& edu_org_ids,
      const GraphView<int64_t>& user_edu_org_view) const {
    for (auto edge : user_edu_org_view.get_edges(user_vid)) {
      auto dst = edge.get_neighbor();
      // binary search on edu_org_ids
      if (std::binary_search(edu_org_ids.begin(), edu_org_ids.end(), dst)) {
        return true;
      }
    }
    return false;
  }

  void init_user_friend_set(
      vid_t user_vid,
      const GraphView<friend_edge_type>& user_user_friend_view_oe,
      const GraphView<friend_edge_type>& user_user_friend_view_ie) {
    CHECK(!is_user_friend_inited_);
    for (auto& edge : user_user_friend_view_oe.get_edges(user_vid)) {
      auto dst = edge.get_neighbor();
      users_are_friends_.insert(dst);
    }
    for (auto& edge : user_user_friend_view_ie.get_edges(user_vid)) {
      auto dst = edge.get_neighbor();
      users_are_friends_.insert(dst);
    }
    is_user_friend_inited_ = true;
  }

  inline bool is_friend(vid_t user_vid) const {
    return users_are_friends_.count(user_vid) > 0;
  }

  // Find the recommend users located in [start_ind, end_ind) from the result
  // without intimacy,
  // the constraint is that
  // 1. the user must be in the same org as the input user,
  // 2. not the friend of user.
  //
  // And find users following the priority of
  // 1. has common friend, common dingGroup
  // 2. potential users.
  // 2.1 (JoinDate's diff is less than 3 year)
  // 2.2 same city
  // 2.3 same roleName.
  //
  // When searching for these users, we sort them internally, but not globally,
  // in order for paging.
  //
  // If no users are found, return an empty vector.
  //
  // AS FIRST ATTEMPT, only try to get common friends.
  std::vector<std::pair<vid_t, RecomReason>> try_without_intimacy(
      ReadTransaction& txn, vid_t vid, int32_t start_ind, int32_t end_ind) {
    std::vector<std::pair<vid_t, RecomReason>> res;
    if (!is_user_in_org_inited_) {
      // If the user_in_org not inited, it means that the orgs have too many
      // employees in edu_orgs.
      std::vector<std::pair<vid_t, RecomReason>> common_friend_users =
          try_get_common_friend_users(txn, vid);
    }
  }

  void serialize(const std::vector<std::pair<vid_t, uint16_t>>& intimacy_users,
                 Encoder& output, int32_t limit) {
    CHECK(intimacy_users.size() <= limit);
    output.put_int(intimacy_users.size());
    for (auto& pair : intimacy_users) {
      output.put_long(pair.first);
      output.put_int(pair.second);  // Only return the recomReason temporarily,
                                    // we will return the detail later.
    }
  }

  void serialize(const std::vector<std::pair<vid_t, RecomReason>>& res,
                 Encoder& output, int32_t limit) {
    CHECK(res.size() <= limit);
    output.put_int(res.size());
    for (auto& pair : res) {
      output.put_long(pair.first);
      output.put_int(
          pair.second.type);  // Only return the recomReason temporarily,
                              // we will return the detail later.
    }
  }

  void serialize(const std::vector<std::pair<vid_t, uint16_t>>& intimacy_users,
                 const std::vector<std::pair<vid_t, RecomReason>>& res,
                 Encoder& output, int32_t limit) {
    auto total_size = res.size() + intimacy_users.size();
    CHECK(total_size <= limit);
    output.put_int(total_size);
    for (auto pair : intimacy_users) {
      output.put_long(pair.first);
      output.put_int(pair.second);
    }
    for (auto& pair : res) {
      output.put_long(pair.first);
      output.put_int(
          pair.second.type);  // Only return the recomReason temporarily,
                              // we will return the detail later.
    }
  }

  void sort_intimacy_users(
      std::vector<std::pair<vid_t, uint16_t>>& intimacy_users) {
    std::sort(intimacy_users.begin(), intimacy_users.end(),
              [](const std::pair<vid_t, uint16_t>& a,
                 const std::pair<vid_t, uint16_t>& b) {
                return a.second > b.second;
              });
  }

 private:
  GraphDBSession& graph_;
  label_t user_label_id_;
  label_t ding_org_label_id_;
  label_t ding_edu_org_label_id_;
  label_t ding_group_label_id_;

  label_t friend_label_id_;
  label_t chat_in_group_label_id_;
  label_t intimacy_label_id_;
  label_t work_at_label_id_;
  label_t study_at_label_id_;

  int32_t edu_org_num_ = 0;
  int32_t users_num_ = 0;

  bool is_user_in_org_inited_ = false;
  bool is_user_friend_inited_ = false;

  // A hash_set to store the users that are in the same org as the input user
  // Can be reused for different stage of recommendation.
  std::unordered_set<vid_t> users_studyAt_edu_org_;
  std::unordered_set<vid_t> users_are_friends_;

  std::vector<vid_t> valid_edu_org_ids_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::AlumniRecom* app = new gs::AlumniRecom(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::AlumniRecom* casted = static_cast<gs::AlumniRecom*>(app);
  delete casted;
}
}