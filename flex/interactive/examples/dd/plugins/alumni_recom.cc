
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
  AlumniRecom(GraphDBSession& graph)
      : graph_(graph),
        user_label_id_(graph.schema().get_vertex_label_id("User")),
        ding_org_label_id_(graph.schema().get_vertex_label_id("DingOrg")),
        ding_edu_org_label_id_(
            graph.schema().get_vertex_label_id("DingEduOrg")),
        ding_group_label_id_(graph.schema().get_vertex_label_id("DingGroup")),
        intimacy_label_id_(graph.schema().get_edge_label_id("Intimacy")),
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        is_user_in_org_inited_(false),
        is_user_friend_inited_(false) {}

  bool Query(Decoder& input, Encoder& output) {
    users_in_org_ids_.clear();
    is_user_in_org_inited_ = false;
    is_user_friend_inited_ = false;

    int64_t user_id = input.get_long();
    int32_t page_id = input.get_int();
    int32_t limit = input.get_int();
    int32_t start_ind = (page_id) *limit;
    int32_t end_ind = (page_id + 1) * limit;
    auto txn = graph_.GetReadTransaction();

    vid_t vid;
    if (!txn.GetVertexIndex(user_label_id_, Any::From(user_id), vid)) {
      LOG(ERROR) << "Vertex not found: " << user_id;
      return false;
    }

    //首先通过亲密度边，拿到所有有亲密度的User
    auto user_user_intimacy_view = txn.GetOutgoingGraphView<FixedChar>(
        user_label_id_, user_label_id_, intimacy_label_id_);
    auto user_edu_org_view = txn.GetOutgoingGraphView<int64_t>(
        user_label_id_, ding_edu_org_label_id_, work_at_label_id_);
    auto user_user_friend_view = txn.GetOutgoingGraphView<int64_t>(
        user_label_id_, user_label_id_, friend_label_id_);

    std::vector<bool> valid_edu_org_ids;
    std::vector<vid_t> edu_org_ids;
    valid_edu_org_ids.resize(edu_org_num_, false);
    {
      for (auto& edge : user_edu_org_view.get_edges(vid)) {
        auto dst = edge.get_neighbor();
        valid_edu_org_ids[dst] = true;
        edu_org_ids.emplace_back(dst);
      }
    }

    std::vector<std::pair<vid_t, uint16_t>> intimacy_users;

    // only keep the user that are in the same org, AND not friend
    traverse_intimacy_view(vid, user_user_intimacy_view, user_user_friend_view,
                           user_edu_org_view, valid_edu_org_ids, edu_org_ids,
                           intimacy_users);
    //

    VLOG(10) << "intimacy_users size: " << intimacy_users.size();
    if (start_ind > intimacy_users.size()) {
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
      sort_intimacy_users(intimacy_users);
      std::vector<std::pair<vid_t, RecomReason>> res =
          try_without_intimacy(txn, vid, 0, end_ind - intimacy_users.size());
      serialize(intimacy_users, res, output, limit);
      txn.Commit();
      return true;
    } else {
      sort_intimacy_users(intimacy_users);
      serialize(intimacy_users, output, limit);
      txn.Commit();
      return true;
    }
  }

  inline bool user_studied_at(vid_t user_vid,
                              const std::vector<bool>& edu_org_ids,
                              const GraphView<int64_t>& user_edu_org_view) {
    for (auto edge : user_edu_org_view.get_edges(user_vid)) {
      auto dst = edge.get_neighbor();
      if (edu_org_ids[dst]) {
        return true;
      }
    }
    return false;
  }

  inline bool is_friend(vid_t user_vid) {
    return users_are_friends_.count(user_vid) > 0;
  }

  // 通过共同好友和共同群，拿到User,这里只保留和当前组织有过关系的用户
  void traverse_intimacy_view(
      vid_t vid, const GraphView<FixedChar>& user_user_intimacy_view,
      const GraphView<int64_t>& user_user_friend_view,
      const GraphView<int64_t>& user_edu_org_view,
      const std::vector<bool>& valid_org_ids,
      const std::vector<vid_t> edu_org_ids,
      std::vector<std::pair<vid_t, uint16_t>>& intimacy_users) {
    // 如果所有edu_org的领边加起来超过阈值，我们将通过用户来访问edu_org.
    // 如果所有edu_org的领边加起来不超过阈值，我们将通过edu_org来访问用户.
    size_t org_employee_cnt = 0;
    {
      for (auto org_id : edu_org_ids) {
        org_employee_cnt +=
            user_edu_org_view.get_edges(org_id).estimated_degree();
      }
    }
    init_user_friend(vid, user_user_friend_view);
    if (org_employee_cnt >= EMPLOYEE_CNT) {
      VLOG(10) << "traverse_intimacy_view: org_employee_cnt: "
               << org_employee_cnt << " >= " << EMPLOYEE_CNT;
      for (auto& edge : user_user_intimacy_view.get_edges(vid)) {
        auto dst = edge.get_neighbor();
        // only keep the user that are in the same org
        if (user_studied_at(dst, valid_org_ids, user_edu_org_view) &&
            !is_friend(dst)) {
          auto fc = edge.get_data();
          auto cur_ptr = static_cast<const char*>(fc.ptr);
          // the first it a uint8_t, the second is a uint16_t
          auto intimacy = *reinterpret_cast<const uint8_t*>(cur_ptr);
          auto comm_score = *reinterpret_cast<const uint16_t*>(cur_ptr + 1);
          intimacy_users.emplace_back(dst, intimacy + comm_score);
        }
      }
    } else {
      LOG(INFO) << "traverse_intimacy_view: org_employee_cnt: "
                << org_employee_cnt << " < " << EMPLOYEE_CNT;
      // In this case, we try to first calculate the bitset indicating whether a
      // person is in the orgs
      init_user_in_org_ids(user_edu_org_view, edu_org_ids);

      for (auto& edge : user_user_intimacy_view.get_edges(vid)) {
        auto dst = edge.get_neighbor();
        if (users_in_org_ids_.count(dst) > 0 && !is_friend(dst)) {
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

  void init_user_in_org_ids(const GraphView<int64_t>& user_edu_org_view,
                            const std::vector<vid_t>& edu_org_ids) {
    CHECK(!is_user_in_org_inited_);
    for (auto org_id : edu_org_ids) {
      for (auto& edge : user_edu_org_view.get_edges(org_id)) {
        auto dst = edge.get_neighbor();
        users_in_org_ids_.insert(dst);
      }
    }
    is_user_in_org_inited_ = true;
  }

  void init_user_friend(vid_t user_vid,
                        const GraphView<int64_t>& user_user_friend) {
    CHECK(!is_user_friend_inited_);
    for (auto& edge : user_user_friend.get_edges(user_vid)) {
      auto dst = edge.get_neighbor();
      users_are_friends_.insert(dst);
    }
    is_user_friend_inited_ = true;
  }

  // Find the recommend users located in [start_ind, end_ind) from the result
  // with intimacy
  std::vector<std::pair<vid_t, RecomReason>> try_without_intimacy(
      const ReadTransaction& txn, vid_t vid, int32_t start_ind,
      int32_t end_ind) {
    std::vector<std::pair<vid_t, RecomReason>> res;
    return res;
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
  std::unordered_set<vid_t> users_in_org_ids_;
  std::unordered_set<vid_t> users_are_friends_;
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