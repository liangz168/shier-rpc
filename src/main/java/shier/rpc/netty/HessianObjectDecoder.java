package shier.rpc.netty;

import com.caucho.hessian.io.Hessian2Input;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-11 下午4:15
 **/
public class HessianObjectDecoder extends LengthFieldBasedFrameDecoder {


    public HessianObjectDecoder(int maxObjectSize) {
        super(maxObjectSize, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        ByteBufInputStream is = new ByteBufInputStream(frame, true);
        Hessian2Input ois = new Hessian2Input(is);
        try {
            return ois.readObject();
        } finally {
            in.release();
            ois.close();
        }
    }
}
