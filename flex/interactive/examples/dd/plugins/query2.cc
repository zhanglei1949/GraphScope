#include <queue>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include <random>
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
class Query2 : public AppBase {
 public:
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
        edu_org_num_(graph.graph().vertex_num(ding_edu_org_label_id_)),
        org_num_(graph.graph().vertex_num(ding_org_label_id_)),
        users_num_(graph.graph().vertex_num(user_label_id_)),
        is_user_in_org_inited_(false),
        is_user_friend_inited_(false) {}

  void get_friends(vid_t root, gs::ImmutableGraphView<grape::EmptyType>& oes,
                   gs::ImmutableGraphView<grape::EmptyType>& ies,
                   std::vector<vid_t>& friends) {
    const auto& oe = oes.get_edges(root);
    friends.clear();
    for (auto& e : oe) {
      friends.emplace_back(e.neighbor);
    }
    const auto& ie = ies.get_edges(root);
    for (auto& e : ie) {
      friends.emplace_back(e.neighbor);
    }
    std::sort(friends.begin(), friends.end());
  }

  bool checkSameOrg(const GraphView<char_array<20>>& work_at,
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
    int64_t oid = input.get_long();
    gs::vid_t root;
    auto txn = graph_.GetReadTransaction();
    graph_.graph().get_lid(user_label_id_, oid, root);
    auto subIndustry = user_subIndustry_col_.get_view(root);
    auto subIndustryIdx = user_subIndustry_col_.get_idx(root);
    if (subIndustry == "") {
      output.put_int(0);
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
        root_work_at.emplace(e.neighbor);
      }
    }
    const auto& intimacy_edges = txn.GetOutgoingImmutableEdges<char_array<4>>(
        user_label_id_, root, user_label_id_, intimacy_label_id_);
    std::vector<vid_t> intimacy_users;

    for (auto& e : intimacy_edges) {
      if (!checkSameOrg(work_at, study_at, root_work_at, root_study_at,
                        e.neighbor))
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
    std::unordered_set<vid_t> vis_set;
    vis_set.emplace(root);
    // 1-hop friends
    for (auto& e : friend_edges_oe) {
      friends.emplace_back(e.get_neighbor());
      vis_set.emplace(e.get_neighbor());
    }
    for (auto& e : friend_edges_ie) {
      friends.emplace_back(e.get_neighbor());
      vis_set.emplace(e.get_neighbor());
    }
    std::sort(friends.begin(), friends.end());
    size_t friend_num =
        std::unique(friends.begin(), friends.end()) - friends.begin();
    friends.resize(friend_num);
    {
      auto len = std::unique(intimacy_users.begin(), intimacy_users.end()) -
                 intimacy_users.begin();
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

    std::vector<vid_t> ans;
    // root -> Intimacy -> Users
    for (auto& v : intimacy_users) {
      if (subIndustryIdx == user_subIndustry_col_.get_idx(v)) {
        ans.emplace_back(v);
        vis_set.emplace(v);
        if (ans.size() > 500) {
          for (auto vid : ans) {
            output.put_long(
                graph_.graph().get_oid(user_label_id_, vid).AsInt64());
          }

          return true;
        }
        // break;
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
    std::unordered_map<uint32_t, int> mp;
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
          if (e.neighbor != root &&
              subIndustryIdx == user_subIndustry_col_.get_idx(e.neighbor)) {
            mp[e.neighbor] += 1;
          }
        }
      }
    }
    std::vector<std::pair<int, vid_t>> users;
    // 2-hop friends
    if (friends.size()) {
      for (size_t i = 0; i < friends.size(); ++i) {
        auto cur = friends[i];
        const auto& ie = friends_ie.get_edges(cur);
        const auto& oe = friends_oe.get_edges(cur);
        for (auto& e : ie) {
          if (e.neighbor != root &&
              subIndustryIdx == user_subIndustry_col_.get_idx(e.neighbor)) {
            mp[e.neighbor] += 1;
          }
        }
        for (auto& e : oe) {
          if (e.neighbor != root &&
              subIndustryIdx == user_subIndustry_col_.get_idx(e.neighbor)) {
            mp[e.neighbor] += 1;
          }
        }
      }
    }
    for (auto& [a, b] : mp) {
      users.emplace_back(b, a);
    }
    std::sort(users.begin(), users.end());
    size_t idx = users.size();
    while (ans.size() < 500 && idx > 0) {
      auto vid = users[idx - 1].second;

      if (!vis_set.count(vid)) {
        vis_set.emplace(vid);
        if (!checkSameOrg(work_at, study_at, root_work_at, root_study_at,
                          vid)) {
          ans.emplace_back(vid);
        }
      }
      --idx;
    }

    if (ans.size() < 500) {
      int limit = 300000;
      const auto& involved_ie = txn.GetIncomingImmutableEdges<grape::EmptyType>(
          subIndustry_label_id_, subIndustryIdx, user_label_id_,
          involved_label_id_);
      auto root_city = user_city_col_.get_view(root);
      bool exist_city = true;
      if(root_city == " "){
	      exist_city = false;
      }
      auto root_city_id = user_city_col_.get_idx(root);
      auto root_roleName_id = user_roleName_col_.get_view(root);
       std::random_device rd; // Seed with a real random value, if available
       std::mt19937 eng(rd()); // A Mersenne Twister pseudo-random generator of 32-bit numbers
	       // Define the range of numbers you want, here it's 0 through 99
     std::uniform_int_distribution<int> dist(0, 500);
      std::vector<uint32_t> tmp;
      if (involved_ie.estimated_degree() > 1) {
        for (auto& e : involved_ie) {
          --limit;
	  if(limit == 0)break;
          if (!vis_set.count(e.neighbor)) {
            auto city_id = user_city_col_.get_idx(e.neighbor);
            auto roleName_id = user_roleName_col_.get_view(e.neighbor);
            if ((!exist_city||city_id != root_city_id) && roleName_id != root_roleName_id&&limit > 0){
		    if(tmp.size() < 500){
		    	tmp.emplace_back(e.neighbor);
		    }else{
			auto idx = dist(eng);
			if(idx < 500){
				tmp[idx] = e.neighbor;
			}
		    }
	    }
            if (!checkSameOrg(work_at, study_at, root_work_at, root_study_at,
                              e.neighbor)) {
              ans.emplace_back(e.neighbor);
              if (ans.size() >= 500) {
                break;
              }
            }
          }
	  if(limit == 0){
//		  LOG(INFO) << "cur ans size: " << ans.size() << "\n";
	  }
        }
      }
      size_t idx = 0;
      std::sort(tmp.begin(),tmp.end());
      size_t len = std::unique(tmp.begin(),tmp.end()) - tmp.begin();
      while(idx < len && ans.size() < 500){
	      auto v = tmp[idx++];
	      if(!checkSameOrg(work_at,study_at,root_work_at,root_study_at,v)){
		      ans.emplace_back(v);
	      }
      }
    }
 

    //std::cout << "user size: " << users.size() << " ans size: " << ans.size()
              //<< "\n";
    for (auto vid : ans) {
      output.put_long(graph_.graph().get_oid(user_label_id_, vid).AsInt64());
    }

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
  gs::TypedColumn<uint8_t>& user_roleName_col_;
  size_t edu_org_num_ = 0;
  size_t users_num_ = 0;
  size_t org_num_ = 0;

  bool is_user_in_org_inited_ = false;
  bool is_user_friend_inited_ = false;
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
