#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {
namespace runtime {

static PropertyType get_vertex_pk_type(const Schema& schema, label_t label) {
  const auto& pk_types = schema.get_vertex_primary_key(label);
  CHECK(pk_types.size() == 1) << "Only support one primary key";
  return std::get<0>(pk_types[0]);
}

WriteContext eval_load(const cypher::Load& opr, GraphInsertInterface& graph,
                       WriteContext&& ctx,
                       const std::map<std::string, std::string>& params) {
  CHECK(opr.kind() == cypher::Load_Kind::Load_Kind_CREATE)
      << "Only support CREATE";
  auto& mappings = opr.mappings();
  int vertex_mapping_size = mappings.vertex_mappings_size();
  int edge_mapping_size = mappings.edge_mappings_size();
  const auto& schema = graph.schema();

  for (int i = 0; i < vertex_mapping_size; ++i) {
    const auto& vertex_mapping = mappings.vertex_mappings(i);
    const auto& vertex_label = vertex_mapping.type_name();
    const auto& vertex_label_id = schema.get_vertex_label_id(vertex_label);
    auto pk_type = get_vertex_pk_type(schema, vertex_label_id);

    const auto& vertex_prop_types =
        schema.get_vertex_properties(vertex_label_id);
    const auto& prop_map =
        schema.get_vprop_name_to_type_and_index(vertex_label_id);

    const auto& props = vertex_mapping.column_mappings();
    int prop_size = vertex_mapping.column_mappings_size();
    Any id;
    std::vector<WriteContext::WriteParamsColumn> properties(
        vertex_prop_types.size());
    WriteContext::WriteParamsColumn id_col;
    for (int j = 0; j < prop_size; ++j) {
      const auto& prop = props[j];
      const auto& prop_name = prop.property().key().name();
      if (prop_name == "id") {
        int idx = prop.column().index();
        id_col = ctx.get(idx);
      } else {
        const auto& prop_idx = prop_map.at(prop_name).second;
        const auto& prop_value = ctx.get(prop.column().index());
        properties[prop_idx] = prop_value;
      }

      /// const auto& prop_name = prop.name();
    }
    for (int i = 0; i < ctx.row_num(); ++i) {
      std::vector<Any> props;
      for (size_t j = 0; j < properties.size(); ++j) {
        props.push_back(properties[j].get(i).to_any(vertex_prop_types[j]));
      }
      graph.AddVertex(vertex_label_id, id_col.get(i).to_any(pk_type), props);
    }
  }

  for (int i = 0; i < edge_mapping_size; ++i) {
    const auto& edge_mapping = mappings.edge_mappings(i);
    const auto& type_triplet = edge_mapping.type_triplet();
    const auto& src_label = type_triplet.source_vertex();
    const auto& dst_label = type_triplet.destination_vertex();
    const auto& edge_label = type_triplet.edge();

    const auto src_label_id = schema.get_vertex_label_id(src_label);
    const auto dst_label_id = schema.get_vertex_label_id(dst_label);
    const auto edge_label_id = schema.get_edge_label_id(edge_label);
    const auto& prop_names = schema.get_edge_property_names(
        src_label_id, dst_label_id, edge_label_id);
    const auto& prop_types =
        schema.get_edge_properties(src_label_id, dst_label_id, edge_label_id);
    CHECK(edge_mapping.source_vertex_mappings_size() == 1);
    CHECK(edge_mapping.destination_vertex_mappings_size() == 1);
    auto src_mapping = edge_mapping.source_vertex_mappings(0);
    auto dst_mapping = edge_mapping.destination_vertex_mappings(0);

    CHECK(src_mapping.property().key().name() == "id");
    CHECK(dst_mapping.property().key().name() == "id");

    auto src_pk_type = get_vertex_pk_type(schema, src_label_id);
    auto dst_pk_type = get_vertex_pk_type(schema, dst_label_id);
    // get src and dst vertex id
    auto src = ctx.get(src_mapping.column().index());
    auto dst = ctx.get(dst_mapping.column().index());

    const auto& edge_props = edge_mapping.column_mappings();
    CHECK(static_cast<size_t>(edge_mapping.column_mappings_size()) ==
          prop_types.size())
        << "Only support one property";

    // const auto& props_type =
    //   schema.get_edge_properties(src_label, edge_label, dst_label);
    CHECK(prop_names.size() < 2) << "Only support one property";
    if (prop_types.size() == 0) {
      for (int i = 0; i < ctx.row_num(); ++i) {
        graph.AddEdge(src_label_id, src.get(i).to_any(src_pk_type),
                      dst_label_id, dst.get(i).to_any(dst_pk_type),
                      edge_label_id, Any());
      }
    } else {
      const auto& edge_prop = edge_props[0];
      const auto& prop_name = edge_prop.property().key().name();
      CHECK(prop_name == prop_names[0]) << "property name not match";
      const auto& prop_type = prop_types[0];
      const auto& prop_value = ctx.get(edge_prop.column().index());
      for (int i = 0; i < ctx.row_num(); ++i) {
        graph.AddEdge(src_label_id, src.get(i).to_any(src_pk_type),
                      dst_label_id, dst.get(i).to_any(dst_pk_type),
                      edge_label_id, prop_value.get(i).to_any(prop_type));
      }
    }

    // LOG(FATAL) << opr.DebugString();
  }
  return ctx;
}
}  // namespace runtime
}  // namespace gs