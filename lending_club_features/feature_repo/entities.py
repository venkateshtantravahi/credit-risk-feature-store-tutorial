from feast import Entity, ValueType

customer = Entity(name="loan_id", join_keys=["loan_id"], value_type=ValueType.STRING)

state = Entity(name="state", join_keys=["state"], value_type=ValueType.STRING)
