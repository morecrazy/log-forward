package kafka

type ConsumerMetadataRequest struct {
	ConsumerGroup string
}

func (r *ConsumerMetadataRequest) encode(pe packetEncoder) error {
	return pe.putString(r.ConsumerGroup)
}

func (r *ConsumerMetadataRequest) decode(pd packetDecoder) (err error) {
	r.ConsumerGroup, err = pd.getString()
	return err
}

func (r *ConsumerMetadataRequest) key() int16 {
	return 10
}

func (r *ConsumerMetadataRequest) version() int16 {
	return 0
}
