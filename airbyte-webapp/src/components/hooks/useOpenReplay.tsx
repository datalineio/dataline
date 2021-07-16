import { useMemo } from "react";
import OpenReplay from "@openreplay/tracker";

const useOpenReplay = (projectKey: string): OpenReplay => {
  return useMemo(() => {
    const tracker = new OpenReplay({
      projectKey: projectKey,
    });

    tracker.start();

    return tracker;
  }, [projectKey]);
};

export default useOpenReplay;
